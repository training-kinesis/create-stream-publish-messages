import { Kinesis } from '@aws-sdk/client-kinesis'
import { promisify } from 'util';
import {Axios} from 'axios';
import { execSync } from 'child_process';

const kinesis = new Kinesis({ region: 'eu-west-1' });
const STOP_PROCCESS_TIME = 5;

function listKinesisStreams (params = {}) {
    const listStreams = promisify(kinesis.listStreams).bind(kinesis);

    return listStreams(params)
    .then(data => data.StreamNames)
}

function createStream (streamName) {

    if (!streamName) {
        return;
    }

    const createStream = promisify(kinesis.createStream).bind(kinesis);

    return createStream({
        StreamName: streamName,
        ShardCount: 2
    })
}

function getPokemon( pokeId ) {
    const axios = new Axios({
        baseURL: 'https://pokeapi.co/api/v2',
    })
    return axios.get(`/pokemon/${pokeId}`)
        .then(res => Buffer.from(res.data))
}

function stopProccess () {
    execSync(`sleep ${STOP_PROCCESS_TIME}`)
    return;
}

function sendData (streamName, data, order) {
    const putRecord = promisify(kinesis.putRecord).bind(kinesis);

    return putRecord({
        StreamName: streamName,
        Data: data,
        PartitionKey: new Date().getTime().toString(),
        SequenceNumberForOrdering: `${order}`
    });
}

async function main (streamName) {
    let counter = 1;
    const params = {
        StreamName: streamName,
        Limit: 10
    };

    const listStreams = await listKinesisStreams(params);

    if ( listStreams.length === 0 || !listStreams.find(stream => stream === streamName)) {
        await createStream(streamName)
        stopProccess();
    }
    
    while (true) {
        const pokemondata = await getPokemon(counter);
        console.log('NNN Data to push to Kinesis: ', pokemondata);
        const pushData = await sendData(streamName, pokemondata, counter++);
        console.log('NNN Data pushed to Kinesis: ', pushData);
        stopProccess();
    }
}

main(process.argv[2])

