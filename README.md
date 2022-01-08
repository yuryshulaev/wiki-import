# wiki-import — Wikipedia AST to LevelDB

Parses pages from a Wikipedia XML dump into JSON AST using [wikiparse](https://github.com/yuryshulaev/wikiparse) and saves them into a LevelDB database.

## Installation

```
npm i -g wiki-import
```

## Usage

Download and unpack a **\*-pages-articles.xml.bz2** archive of your choosing from [Wikimedia Downloads](https://dumps.wikimedia.org/backup-index.html). Make sure you have enough free space for the database and run:

```
wiki-to-leveldb <pages-articles.xml[.bz2]> <dbPath> [workerCount = cpuCount] [--with-source]
```

for example:

```
lbzip2 -kd simplewiki-20220101-pages-articles.xml.bz2
wiki-to-leveldb simplewiki-20220101-pages-articles.xml simplewiki
```

The first step is optional: if you want to trade time for storage space, you can pass an **\*.xml.bz2** archive directly to `wiki-to-leveldb` for streaming decompression, that will be about 1.5 times slower — `lbzip2` is highly parallel.

It takes about 4 minutes to import **simplewiki** (0.9 GB dump) on tmpfs on a 4-core CPU.
