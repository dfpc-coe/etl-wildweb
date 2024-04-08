import { Static, Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import ETL, { Event, SchemaType } from '@tak-ps/etl';

const Environment = Type.Object({
    'DispatchCenters': Type.Array(Type.Object({
        CenterCode: Type.Optional(Type.String({
            description: 'The Shortcode for the WildWeb Dispatch Center'
        })),
    }), {
        description: 'Inreach Share IDs to pull data from',
        display: 'table',
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
})

export default class Task extends ETL {
    static async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Environment;
        } else {
            return Type.Object({
                incidentDate: Type.String({ format: 'date-time', description: 'Incident Date' }),
                incidentName: Type.String({ description: 'Incident Name' }),
                incidentType: Type.String({ description: 'Incident Type' }),
                incidentUuid: Type.String({ description: 'Incident UUID' }),
                incidentAcres: Type.String({ description: 'Incident Acres' }),
                incidentFuels: Type.String({ description: 'Incident Fuels' }),
                incidentIncNum: Type.String({ description: 'Incident Incident Num' }),
                incidentFireNum: Type.String({ description: 'Incident Fire Num' }),
                incidentComment: Type.String({ description: 'Incident Web Comment' })
            })
        }
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();

        const env = layer.environment as Static<typeof Environment>;
        if (!env.DispatchCenters) throw new Error('No DispatchCenters Provided');
        if (!Array.isArray(env.DispatchCenters)) throw new Error('DispatchCenters must be an array');

        const obtains = [];

        for (const center of env.DispatchCenters) {
            obtains.push((async (center): Promise<Feature[]> => {
                console.log(`ok - requesting ${center.CenterCode}`);

                const url = new URL(`/centers/${center.CenterCode}/incidents`, 'https://snknmqmon6.execute-api.us-west-2.amazonaws.com')

                const centerres = await fetch(url);
                const json = await centerres.json();
                const body = json[0].data;

                const features: Feature[] = [];

                console.log(`ok - ${center.CenterCode} has ${body.length} messages`);

                for (const fire of body) {
                    const feat: Feature<Geometry, Record<string, string>> = {
                        id: `wildweb-${fire.uuid}`,
                        type: 'Feature',
                        properties: {
                            callsign: fire.name,
                            time: new Date(fire.date).toJSON(),
                            start: new Date(fire.date).toJSON()
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: [ Number(fire.longitude) * -1, Number(fire.latitude) ]
                        }
                    };

                    features.push(feat);
                }

                return features;
            })(center))
        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        }

        for (const res of await Promise.all(obtains)) {
            if (!res || !res.length) continue;
            fc.features.push(...res);
        }

        await this.submit(fc);
    }
}

const handler = Task.handler;
await Task.local(import.meta.url);
export { handler };

