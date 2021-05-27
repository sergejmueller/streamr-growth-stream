require('dotenv').config()
const https = require('https')
const streamr = require('streamr-client')

const getRemoteJson = async url => {
    return new Promise((resolve, reject) => {
        https.get({
            host: 'www.streamr.com',
            path: `/api/v1${url}`,
        }, response => {
            let body = ''

            response.on('data', chunk => body += chunk)
            response.on('end', () => resolve(JSON.parse(body)))
        }).on('error', (error) => {
            reject(new Error(`Fetch error: ${error}`))
        })
    })
}

const isStreamActive = (recentMessageTimestamp, inactivityThresholdHours) => {
    return (new Date().getTime() - (inactivityThresholdHours * 60 * 60 * 1000) < recentMessageTimestamp)
}

const getProducts = async () => {
    return new Promise(async (resolve) => {
        const products = await getRemoteJson('/products?publicAccess=true')

        resolve({
            items: products,
            metrics: {
                total: products.length,
                isDataUnion: products.filter(product => product.type === 'DATAUNION').length,
                isNormal: products.filter(product => product.type === 'NORMAL').length,
                isFree: products.filter(product => product.isFree === true).length,
                isNotFree: products.filter(product => product.isFree === false).length,
            }
        })
    })
}

const getStreams = async (products) => {
    return new Promise(async (resolve) => {
        const metrics = {
            total: 0,
            isFree: 0,
            isNotFree: 0,
            isFreeAndNotEmptyAndActive: 0,
            isFreeAndNotEmptyAndNotActive: 0,
            isFreeAndEmpty: 0,
            isFreeAndNotEmpty: 0,
        }

        const recentMessage = x => new Promise(resolve => setTimeout(async () => {
            let message = await getRemoteJson(`/streams/${encodeURIComponent(x.id)}/data/partitions/0/last?count=1`)

            ++ metrics.total

            if (message.length) {
                ++ metrics.isFree
                ++ metrics.isFreeAndNotEmpty

                const isActive = isStreamActive(message[0]['timestamp'], x['inactivityThresholdHours'])

                isActive ? ++ metrics.isFreeAndNotEmptyAndActive : ++ metrics.isFreeAndNotEmptyAndNotActive
            } else {
                if (message.error) {
                    ++ metrics.isNotFree
                } else {
                    ++ metrics.isFree
                    ++ metrics.isFreeAndEmpty
                }
            }

            return resolve()
        }, 1))

        await Promise.all(
            products.map(async product => {
                const streams = await getRemoteJson(`/products/${product.id}/streams`)

                for (let stream of streams.map(x => () => recentMessage(x))) await stream()
            })
        )

        return resolve({metrics})
    })
}

const publishStreamEvent = async (streamId, data) => {
    return new Promise( (resolve, reject) => {
        if (!process.env['STREAMR_PRIVATE_KEY']) {
            reject(new Error('Streamr Private Key is required'))
        }

        const client = new streamr({
            auth: {
                privateKey: process.env['STREAMR_PRIVATE_KEY']
            }
        })

        client.publish(streamId, data)
            .then(() => resolve(data))
            .catch(error => reject(error))
    })
}

(async () => {
    console.log('... ', new Date().toLocaleString());

    try {
        const products = await getProducts()
        await publishStreamEvent(
            process.env['STREAMR_GROWTH_PRODUCTS_STREAM_ID'],
            products.metrics
        )

        const streams = await getStreams(products.items)
        await publishStreamEvent(
            process.env['STREAMR_GROWTH_STREAMS_STREAM_ID'],
            streams.metrics
        )

        console.log(products.metrics)
        console.log(streams.metrics)
    } catch(error) {
        console.log(error)
    }

    process.exit()
})()
