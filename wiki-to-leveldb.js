#!/usr/bin/env node
'use strict'

const cluster = require('cluster');
const fs = require('fs');
const path = require('path');
const os = require('os');
const {Transform} = require('stream');
const bz2 = require('unbzip2-stream');
const XmlStream = require('xml-stream');
const levelup = require('levelup');
const leveldown = require('leveldown');
const {WikiParser} = require('wikiparse');
const ProgressBar = require('./progress/node-progress.js');

function usage() {
	console.error(`Usage: ./${path.basename(process.argv[1])} <pages-articles.xml[.bz2]> <dbPath> [workerCount = cpuCount] [--with-source]`);
	console.error('Imports Wikipedia XML dump <pages-articles.xml[.bz2]> into a LevelDB database <dbPath> (created automatically).');
	process.exit(1);
}

function argsExtractSwitch(args, arg) {
	const index = args.indexOf(arg);

	if (index !== -1) {
		args.splice(index, 1);
		return true;
	}

	return false;
}

const args = process.argv.slice(2);
const kWithSource = argsExtractSwitch(args, '--with-source');

if (args.length < 2 || args.length > 3) {
	usage();
}

const [kDumpFilename, kLevelDbPath, kWorkerCountArg = os.cpus().length] = args;
const kWorkerCount = Number(kWorkerCountArg);

if (isNaN(kWorkerCount)) {
	usage();
}

const kMaxPagesInFlight = 10000;
const kBatchSize = 100;

function capitalizeFirst(string) {
	return string !== '' ? string[0].toUpperCase() + string.substr(1) : '';
}

class CounterTransform extends Transform {
	constructor(options) {
		super(options);
		this.byteCount = 0;
	}

	_transform(chunk, encoding, callback) {
		this.byteCount += chunk.length;
		this.push(chunk);
		callback();
	}
}

if (cluster.isMaster) {
	const dumpFileSize = fs.statSync(kDumpFilename).size;
	const streamOptions = {highWaterMark: 8192};
	const counterTransform = new CounterTransform(streamOptions);
	const inputStream = fs.createReadStream(kDumpFilename, streamOptions).pipe(counterTransform);
	const stream = (kDumpFilename.match(/\.bz2$/i) ? inputStream.pipe(bz2()) : inputStream);
	const levelOptions = {writeBufferSize: 32 << 20};
	const db = levelup(leveldown(kLevelDbPath), levelOptions);

	function formatNumber(value, fractionDigits = 0) {
		const precision = 0.1 ** (1 + fractionDigits);
		return ((((value / precision) / (1 << 20)) | 0) * precision).toLocaleString(
			'en-US', {minimumFractionDigits: fractionDigits, maximumFractionDigits: fractionDigits});
	}

	const bar = new ProgressBar('xml → leveldb [:bar] :percent :currentmb/:totalmb :ratemb/s elapsed :elapsed eta :eta', {
		total: dumpFileSize,
		width: 50,
		renderThrottle: 100,
		formatValue: x => formatNumber(x),
		formatRate: x => formatNumber(x, 1),
	});

	let nRead = 0;
	let nWritten = 0;
	let batch = [];

	function write({key, value}) {
		batch.push({type: 'put', key, value});

		if (batch.length >= kBatchSize) {
			const currentBatch = batch.splice(0);

			db.batch(currentBatch, function (err) {
				if (err) {
					console.error(err);
					process.exit(1);
				}

				if ((nWritten += currentBatch.length) >= nRead - kMaxPagesInFlight) {
					stream.resume();
				}
			});
		}
	}

	const workers = [];

	for (let i = 0; i < kWorkerCount; i++) {
		const worker = cluster.fork();
		workers.push(worker);
		worker.on('message', write);
	}

	cluster.on('death', function (worker) {
		console.error('Worker', worker.pid, 'died');
		process.exit(1);
	});

	const xml = new XmlStream(stream);
	xml._preserveAll = true; // Keep newlines
	let nSentToWorkers = 0;

	xml.on('endElement: page', page => {
		let title;

		try {
			title = page.title.trim();
			const source = page.revision.text.$text || '';

			if (!/^(?!(Category|Категория):)\p{L}+:\w/ui.test(title)) {
				const redirectMatch = source.match(/^#redirect:?\s*\[\[(.+)\]\]/i);

				if (redirectMatch) {
					write({key: capitalizeFirst(title), value: JSON.stringify({id: page.id, title, redirectTo: capitalizeFirst(redirectMatch[1]),
						...(kWithSource ? {source} : {})})});
				} else {
					workers[nSentToWorkers % workers.length].send({id: page.id, title, source});
					nSentToWorkers++;
				}

				if (++nRead >= nWritten + kMaxPagesInFlight) {
					stream.pause();
				}
			}

			bar.tick(counterTransform.byteCount - bar.curr);
		} catch (err) {
			console.error(title, err);
			process.exit(1);
		}
	});

	stream.on('close', () => {
		bar.terminate();
		console.log('Shutting down...');

		for (const worker of workers) {
			worker.send({command: 'disconnect'});
		}
	});

	xml.on('error', message => {
		console.error('XML parsing failed:', message);
	});
} else {
	const parser = new WikiParser({backtrackingLimit: 50000, returnError: true});

	process.on('message', ({command, id, title, source}) => {
		if (command === 'disconnect') {
			process.disconnect();
			return;
		}

		const startTime = Date.now();
		let ast = parser.parse(source);

		if (ast instanceof Error) {
			console.error(title, ast);
			ast = [];
		}

		const parseTime = (Date.now() - startTime) / 1000;
		const page = {id, title, ast, parseTime, backtrackingCount: parser.backtrackingCount, ...(kWithSource ? {source} : {})};
		process.send({key: capitalizeFirst(title), value: JSON.stringify(page)});
	});
}
