/**
 *
 * @typedef {{
 *    url: string,
 *    description: string,
 *    deact: number,
 *    p1: number,
 *    p2: number,
 *    p3: number,
 *    p4: number,
 *    p5: number,
 *    p6: number,
 *    p7: number,
 *    p8: number,
 *    p9: number,
 *    p10: number,
 *    p99: number
 * }} HeritrixSeed
 *
 * @typedef {{
 *   entityLabel: {value: string, key: string, [description]: string}[],
 *   entityDescription: string,
 *   entityName: string,
 *   uri: string,
 *   seedDescription: string
 *   seedLabel: {value: string, key: string, [description]: string}[],
 * }} Veidemannctl
 *
 *  @typedef {{
 *      url: string,
 *      institusjon: string
 *  }} School
 */

/**
 *
 * Script creates a .json file of objects to insert as entites and seeds in Veidemann.
 *
 * @param source
 * The script will use a JSON array dump of the heritrix database.
 * The scripts uses data from these objects to create a new set of JSON objects, suited for use with veidemannctl.
 * The new object will not be created if the URL og entityName are invalid.
 *
 * @output
 *  After running the script, the following files will be created:
 *  output - File with JSON objects to import to veidemann
 *  errorUrls - File with URL that the script was not able to validate
 *  failed - File with JSON objects thats now valid and therefore not added to the output file
 *
 *
 */


const StreamArray = require('stream-json/streamers/StreamArray');
const path = require('path');
const fs =  require('fs');
const {parse} = require ('url');
const source = './input/heritrixSeeds.json';
const schoolList = require("./skolerOgUniversiteter");
const output = './output/veidemannSeeds.json';
const errorUrls = './output/failed_heritrix_url.txt';
const failed = './output/failed_heritrix_seeds.json';

const inputStream = StreamArray.withParser();
fs.createReadStream(path.join(__dirname, source)).pipe(inputStream.input);

const outputStream = fs.createWriteStream(output);
const errorStream = fs.createWriteStream(errorUrls);
const invalidSeedStream = fs.createWriteStream(failed);

const t0  = Date.now();

let heritrixSeedsChecked = 0;
let createdVeidemannSeeds = 0;
let invalidUrl = 0;
let invalidName = 0;


inputStream.on('data', ({key, value}) => {
    heritrixSeedsChecked++;
    if (value.url) {
        const veidemannSeed = transform(value);
        if (veidemannSeed.entityName && veidemannSeed.uri) {
            createdVeidemannSeeds++;
            outputStream.write(JSON.stringify(veidemannSeed) + '\n');
        } else {
            if (!veidemannSeed.entityName) {
                invalidName ++;
            }

            if (!veidemannSeed.uri) {
                invalidUrl++;
            }

            invalidSeedStream.write(JSON.stringify(veidemannSeed) + '\n');
        }
   }
});

inputStream.on('end', () => {
   outputStream.end();
   errorStream.end();
   invalidSeedStream.end();

   const t1 = Date.now();

   console.log('*****************************************************************************************************');
   console.log('Har sjekket: ', heritrixSeedsChecked, ' seeds fra heritrix');
   console.log('Av disse ble det opprettet: ', createdVeidemannSeeds, ' seeds til Veidemann');
   console.log(invalidUrl, ' seeds med ugyldige url og ', invalidName, ' med ugyldig navn ble funnet');
   console.log('Disse blir ikke med i import, og kan ses i filen: ', failed );
   console.log('*****************************************************************************************************');
   console.log('Operasjonen tok: ', (t1 - t0)/1000, 'sekunder å gjennomføre');
   console.log('*****************************************************************************************************');
});


/**
 *
 * @param {HeritrixSeed} seed
 * @returns {{entityLabel: *, entityDescription: null, entityName: *, seedLabel: *, uri: *, seedDescription: string}}
 */

function transform(seed) {
    return {
        entityName: getEntityName(seed.url),
        uri: getUri(seed.url),
        entityDescription:  seed.description,
        entityLabel: getEntityLabel(seed),
        seedLabel: getSeedLabel(seed),
        seedDescription: ''
    }
}


/**
 *
 * Set entityName based on seed.url
 *
 * @param urlString
 * @returns {string}
 */
function getEntityName(urlString) {
    const seed = parse(urlString);
    let name = " ";

    let hostname = "";
    if (!seed.protocol) {
        try {
            const seedWithProtocol = parse('http://' + urlString);

            if (seedWithProtocol.protocol && seedWithProtocol.slashes && seedWithProtocol.hostname) {
                hostname = seedWithProtocol.hostname;
            }
        } catch (err) {
            console.error(err);
        }
    } else {
        hostname = seed.hostname;
    }

    if (hostname) {
        if (hostname.includes('.')) {
            name = hostname.split('.');
            if (name.length > 1) {
                name.pop();
                if (name[0].includes('www')) {
                    name.splice(0, 1);
                }
            }
        }
    } else {
        errorStream.write
        ('Could not create entityname based on hostname from url: ' +  urlString + JSON.stringify(seed) + '\n');
        return undefined;
    }
    if (name.length > 1) {
        return name.join(' ').toString().charAt(0).toUpperCase() + name.join(' ').toString().slice(1);
    } else {
        return name.toString().charAt(0).toUpperCase() + name.toString().slice(1);
    }

}

/**
 *
 * @param {HeritrixSeed} seed
 * @returns entityLabel: *
 */

function getEntityLabel(seed) {

    let labels = [];

    labels.push({
        "key": "source",
        "value": "heritrix"
    });

    if (seed.description == null) {
        seed.description = "";
    }

    // create label for entity type based on seed.url and seed.description

    if (seed.url !== undefined || seed.url === "") {

        if (seed.url.includes('blog')) {
            labels.push({
                "key": "næring",
                "value": "blogg"
            });
        }
        if (seed.url.includes('avis') || seed.url.includes('posten') || seed.url.includes('tidende') || seed.url.includes('blad')) {
            labels.push({
                "key": "næring",
                "value": "avis"
            });
        }

        if (seed.url.includes('kommune')) {
            labels.push({
                "key": "næring",
                "value": "kommune"
            });
        }

        if (seed.url.includes('twitter')) {
            labels.push({
                "key": "næring",
                "value": "twitter"
            });
        }

        if (seed.url.includes('fylkeskommune') || seed.description.includes('fylkeskommune')) {
            labels.push({
                "key": "næring",
                "value": "fylkeskommune"
            });
        }


        if (seed.url.includes('parti') || seed.description.includes('parti')) {
            labels.push({
                "key": "næring",
                "value": "politisk parti"
            });
        }

        if (seed.url.includes('teater') || seed.url.includes('theater') || seed.description.includes('teater') || seed.description.includes('theater')) {
            labels.push({
                "key": "næring",
                "value": "teater"
            });
        }

        if (seed.url.includes('museum') || seed.description.includes('museum')) {
            labels.push({
                "key": "næring",
                "value": "museum"
            });
        }

        for (const school of schoolList) {
            if (seed.url.includes(school.url)) {
                labels.push({
                    "key": "næring",
                    "value": school.institusjon
                });
            }
        }

        return labels;

    }
}

/**
 *
 * @param {HeritrixSeed} seed
 * @returns entityLabel: *
 */

function getSeedLabel(seed) {

    let labels = [];

    labels.push({
        'key':'source',
        'value':'heritrix'
    });

    if (hasProfiles(seed) !== null) {
        labels.push({
            "key": "heritrix_profile",
            "value": hasProfiles(seed)
        });
    }

    return labels;

}

function getUri(urlString) {
    const httpProtocol = 'http://';

    let parsedUrl = {};
    try {
        parsedUrl = parse(urlString);
    } catch (err) {
        console.error(err);
    }

    if (!parsedUrl.protocol) {
        try {
            const seedUrl = parse(httpProtocol + urlString);

            if (seedUrl.protocol && seedUrl.slashes && seedUrl.host && seedUrl.href) {
                return seedUrl.href;
            } else {
                errorStream.write(urlString + ': ' + JSON.stringify(seedUrl) + '\n');
            }
        } catch (err) {
            console.error(err);
        }
    } else {
        if (parsedUrl.protocol && parsedUrl.slashes && parsedUrl.host && parsedUrl.href) {
            return parsedUrl.href;
        } else {
            errorStream.write(JSON.stringify(parsedUrl) + '\n');
        }
    }
}

function hasProfiles(seed) {
    const profiles = [];
    if (seed.p1 === 1) {
        profiles.push('p1');
    }
    if (seed.p2 === 1) {
        profiles.push('p2');
    }
    if (seed.p3 === 1) {
        profiles.push('p3');
    }

    if (seed.p4 === 1) {
        profiles.push('p4')
    }

    if (seed.p5 === 1) {
        profiles.push('p5')
    }

    if (seed.p6 === 1) {
        profiles.push('p6')
    }
    if (seed.p7 === 1) {
        profiles.push('p7')
    }
    if (seed.p8 === 1) {
        profiles.push('p8')
    }
    if (seed.p9 === 1) {
        profiles.push('p9')
    }
    if (seed.p10 === 1) {
        profiles.push('p10')
    }
    if (seed.p99 === 1) {
        profiles.push('p99')
    }
    if (profiles.toString() !== "") {
        return profiles.toString();
    } else {
        return null;
    }
}
