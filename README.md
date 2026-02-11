# Nightly-paperpusher

If I had to define nightly-paperpusher with buzzwords I barely know the meaning of,
I'd say that nightly-paperpusher is an integrated maven repository.

Mainly, it aims to be the backbone of the stianloader.org nightly builds pipeline
(hence the name). This is also the reason why ALL artifacts have unique version strings.
This repository software is not capable of working with snapshot artifacts.

## Security

I initially developed this software as a temporary solution - but as many other
temporary solutions go, this solution seems to be rather permament.
However, due to the nature of this being a "temporary" solution, compounded
with my relative inexperience with web development, the codebase behind this
software is relatively atrocious and might contain one or the other security
vulnerability (most likely only path traversal attack vectors though).

Most importantly, this software does next to no AAA (Authentification,
Authorization, Accounting). This is most critical for the maven publish endpoint.
If you absolutely need to expose that part of the repository publicly, please
create an issue on this repository beforehand and use a reverse-proxy
that performs the necessary AAA.

The discord webhook is the only security measure to detect unauthorized
deployments, so please be careful. And also trust your host with your
heart. They can easily exploit the vulnerabilities present in this
application. However, if your host is compromised then you are SOL
anyways, so you have more things to worry about in this case. Nonetheless
this happens, see the Notepad++ intrusion in late 2025.

## Notable features

Besides being something that stores maven artifacts to disk, nightly-paperpusher has
following notable features:

- Artifacts are indexed using the nexus-maven-repository-index
- Javadocs can easily be obtained via an endpoint
- Keeps track of when methods, fields, classes, and packages are added and removed
- This application is developed to be run using graal native-image first and foremost
- Send a discord webhook message when deploying artifacts

## Endpoints

**Note that the maven publication endpoint is intended to be hidden behind a firewall.**
This is caused by the fact that this endpoint should only be used by a
local CI/CD instance.

### Maven publication endpoint

- Default bind prefix: `/maven/`

This is the endpoint that controls the generic publication repository.
This means that maven artifacts can be deployed there, although `-aYYYYMMDD`
will be appended to the version string (if the version already exists, `.X`
will be appended additionally, where as `X` is an incrementing integer).
Snapshot artifacts are not supported.

In order for maven publication to work, maven-metadata.xml files can be fetched
from this endpoint, otherwise no files are readable from this endpoint.
If you wish to have a public-facing maven endpoint, then for the moment
you still need to make use of a different webserver to serve the webpages.

Note: When pushing artifacts to this endpoint they will only get "staged",
but won't be committed to disk until the `<maven-bind-prefix>/commit` endpoint
is called. I.e. by default it would be `/maven/commit`.

This endpoint also allows to post a discord webhook message when committing
to disk, 

### Javadoc endpoint

- Default bind prefix: `/javadocs/`

This is the endpoint from which javadocs are served from.
The javadocs are taken in directly from the configured local maven
repository, and their contents served in an unzipped manner.
Javadocs are listed in an index that is served by navigating to this
endpoint. Note: Do not forget the trailing slash when opening
up the index as otherwise your webbrowser is likely to be confused when
navigating links. 

Javadocs are accessible under the following structure:
`<javadoc-bind-prefix>/<group>/<artifact>/<version>/`
Here, group and artifact are dot separated - so an example link would be:
`/javadocs/org.stianloader/micromixin-annotations/0.4.0-a20240428/`

Groups within the javadoc index list are sorted alphabetically,
same goes for artifact ids. Versions are however sorted according to
maven's version order specification as implemented by picoresolve.

Groups, artifacts and versions that do not have a javadoc
artifact attached to them are discarded and not shown in the javadoc index.

### Search/Delta Endpoint

- Default bind prefix: `/search/`

This is the endpoint on which the version history pages are served from.
The endpoint is also internally responsible for maintianing the maven index files.

The following pages are defined
- `<search-bind-prefix>/projects`
- `<search-bind-prefix>/packages/<group>/<artifact>`
- `<search-bind-prefix>/classes/<group>/<artifact>/<package>`
- `<search-bind-prefix>/members/<group>/<artifact>/<package>/<class>`
- `<search-bind-prefix>/versions/<group>/<version>`
- `<search-bind-prefix>/changelog/<group>/<artifact>/<version>`

Note: Unlike the javadoc endpoint, there is no trailing slash.
`<package>` uses the full stop ('.') character as the package delimiter,
while `<class>` only is the class name. That is for the fully qualified name
of the class `org/stianloader/launcher/Launcher`, `<package>` would be
`org.stianloader.launcher` and `<class>` would be `Launcher`.

The deltas between versions is stored using SQLite in the
`paperpusher-search-indices-v000-0.db` file (file name might
change slightly in future versions as incompatible changes are made).
It is safe to delete the database file whilst the server is offline.
The database file will be regenerated on server startup.

### Administrative endpoints

The two administrative endpoints **- to whom everyone has access!** are follows:

- `/killdaemon` - terminates the process
- `/getpid` - obtains the process ID of the process responsible for serving
 the contents of the webpage

## Nightly-paperpusher versus ??

Unless ?? is a homebrewed solution, then ?? is most likely going to be better
than nightly-paperpusher. If ?? is made at home, then nightly-paperpusher might
be better or might not be better at all - it really depends on what your usecases
are.

## Contributing

If you wish like having more functionality in the programm, then feel free
contribute! If you don't feel like doing so, then it is also permissible to
plainly just ask me - though directly contributing is still the fastest route if you
absolutely need a feature.

## Footprint

If you are using a stock JVM then the memory footprint are expected to
be relatively large. As such, this is not the setup we recommend to use in
production environments. Instead, use GraalVM compiled images that offer
significantly lower memory footprint and faster startup times.
The cost of graal would be a slower repository overall, but that is usually
acceptable unless you have very high throughput (which this software is not designed
to handle anyways).

Just execute the `graal-compile` script with Graal 25 as your current JVM and
you are good to go!
