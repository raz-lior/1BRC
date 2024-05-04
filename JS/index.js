const fs = require('fs');
const readline = require('readline');

function main() {

    const filePath = process.argv[2];
    const fileStream = fs.createReadStream(filePath);
    const lineReader = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let stations = new Map();

    lineReader.on('line', (line) => {
        const lineParts = line.split(';');
        const name = lineParts[0];
        const measurement = parseFloat(lineParts[1]);

        let station = stations.get(name);
        if (station) {
            station.count++;
            station.sum += measurement;

            if (measurement < station.min)
                station.min = measurement;

            if (measurement > station.max)
                station.max = measurement;
        } else {
            stations.set(name, {count: 1, sum: measurement, min: measurement, max: measurement});
        }
    });

    lineReader.on('close', () => {

        let names = [];
        for (let name of stations.keys()) {
            names.push(name);
        }

        names.sort();

        for (let name of names) {
            console.log(`${name}=${stations.get(name).min.toFixed(1)}/${(stations.get(name).sum / stations.get(name).count).toFixed(1)}/${stations.get(name).max.toFixed(1)}`);
        }

    });
}

main();