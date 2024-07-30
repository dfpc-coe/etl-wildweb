import moment from 'moment';
import { Static, Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';

const WildCadIncident = Type.Object({
    ic: Type.Union([Type.String(), Type.Null()]),
    date: Type.String({ format: 'date-time' }),
    name: Type.String(),
    type: Type.String(),
    uuid: Type.String(),
    acres: Type.Union([Type.String(), Type.Null()]),
    fuels: Type.Union([Type.String(), Type.Null()]),
    inc_num: Type.Union([Type.String(), Type.Null()]),
    fire_num: Type.Union([Type.String(), Type.Null()]),
    latitude: Type.Union([Type.String(), Type.Null()]),
    location:  Type.Union([Type.String(), Type.Null()]),
    longitude: Type.Union([Type.String(), Type.Null()]),
    resources: Type.Array(Type.Any()),
    webComment: Type.Union([Type.String(), Type.Null()]),
    fire_status: Type.String(),
    fiscal_data: Type.String(),
});

const Environment = Type.Object({
    IncidentRange: Type.String({
        description: 'Filter Incidents within the follow time range',
        enum: [
            '24 Hours',
            '48 Hours',
            '1 Week'
        ]
    }),
    'DispatchCenters': Type.Array(Type.Object({
        CenterCode: Type.Optional(Type.String({
            description: 'The Shortcode for the WildWeb Dispatch Center'
        })),
    })),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
})

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Environment;
        } else {
            return WildCadIncident;
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        }

        for (const center of env.DispatchCenters) {
            console.log(`ok - requesting ${center.CenterCode}`);

            const url = new URL(`/centers/${center.CenterCode}/incidents`, 'https://snknmqmon6.execute-api.us-west-2.amazonaws.com')

            const centerres = await fetch(url);

            const json = await centerres.typed(Type.Array(Type.Object({
                retrieved: Type.String(),
                data: Type.Union([Type.Null(), Type.Array(WildCadIncident)])
            })));

            if (json.length !== 1) {
                console.error(centerres.headers)
                console.log(`not ok - Unparsable Body: ${center.CenterCode}: ${JSON.stringify(json)}`);
                return;
            }

            const body = json[0].data;

            if (body === null) {
                console.log(`ok - ${center.CenterCode} has 0 messages`);
                continue;
            }

            console.log(`ok - ${center.CenterCode} has ${body.length} messages`);

            for (const fire of body) {
                if (env.IncidentRange) {
                    const duration = parseInt(env.IncidentRange.split(' ')[0]);
                    const unit = env.IncidentRange.split(' ')[1] === 'Hours' ? 'hours' : 'week';

                    if (moment(fire.date).isBefore(moment().subtract(duration, unit))) {
                        continue;
                    }
                }

                fire.date = moment(fire.date).seconds(0).milliseconds(0).toISOString().replace(/:00.000Z/, '').replace('T', ' ');

                // We only pass along valid geospatial data
                if (
                    !fire.longitude || isNaN(Number(fire.longitude)) || Number(fire.longitude) === 0
                    || !fire.latitude || isNaN(Number(fire.latitude)) || Number(fire.latitude) === 0
                ) continue;

                const feat: Feature<Geometry, Record<string, any>> = {
                    id: `wildweb-${fire.uuid}`,
                    type: 'Feature',
                    properties: {
                        callsign: fire.name,
                        metadata: {
                            ...fire
                        }
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [ Number(fire.longitude) * -1, Number(fire.latitude) ]
                    }
                };

                fc.features.push(feat);
            }
        }

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}

