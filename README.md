Musketeer is a workflow manager which can dynamically map front-end workflow
descriptions to a range of back-end execution engines. It is developed by
CamSaS (http://camsas.org) at the University of Cambridge Computer Laboratory.

Musketeer is currently a prototype: it takes as input workflows defined in
several domain specific languages and can generate code for several
back-end execution engines (e.g, Spark, Naiad, Hadoop).

## System requirements

Musketeer is currently known to work on Ubuntu LTS release 14.04 (trusty). Other
configurations are untested.

Moreover, Musketeer currently generates code compatible with the following
back-end execution engine versions:
 * GraphChi 0.2
 * Hadoop 2.0.0-mr1-cdh4.5.0
 * Metis e5b04e2
 * Naiad 0.4
 * PowerGraph 2.2
 * Spark 0.9

Other versions may work if the APIs have not changed too much. If you have added
support for new versions (or just found them to work), please let us know or
send us a pull request.

## Getting started

After cloning the repository,

```console
$ make dependencies
```

fetches dependencies, and may ask you to install required packages.

```console
$ make all
```
If all goes well, you should end up with the Musketeer binary in the build
directory.

Musketeer assumes that back-end execution engines are installed relative to
`MUSKETEER_ROOT`, unless other locations are specified explicitly using flags.
You are free to use any directory for `MUSKETEER_ROOT`. It does not have to be
inside Musketeer's source directory.

In order to test your build you can generate Hadoop code for running PageRank
using the following command:

```console
./build/musketeer --logtostderr --stderrthreshold=0 --run_daemon=false --root_dir=MUSKETEER_ROOT --force_framework=hadoop --dry_run -beer_query=tests/pagerank/page_rank.rap
```
If you already installed Hadoop then if you pass `--dry_run=false` then
Musketeer will run also run the computation.
