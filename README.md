# Important note: this project is no longer actively maintained.

# Flume2Storm Connector

The Flume2Storm Connector is a modular project that bridges [Flume](http://flume.apache.org/) to [Storm](http://storm-project.net/documentation/Home.html), so that Flume events can be forwarded to and processed in Storm. 
The two main components are the storm-sink (in Flume) and the flume-spout (in Storm). The flume-spout connects to the storm-sink, which can then sends the events. In order for the spout to connect the sink, they both use a location service. 

The architectural choice for this connector is to be modular and abstract in the sense that both the location service and the connection framework are API. The storm-sink and the flume-spout configure which implementation of these API to use.


## Documentation

Overview and usage documentation can be found in the [wiki](https://github.com/Comcast/flume2storm/wiki).
The source code contains extensive (javadoc) documentation.


## Presentations

At Comcast Video IP Engineering and Research (VIPER), we designed and have been using this connector for our IP video platform since 2012. We presented the architecture of our real-time stream processing system at Hadoop World 2013. See [the abstract and slides of the presentation](http://strataconf.com/stratany2013/public/schedule/detail/30915). 


## Mailing List

For support, please subscribe and send messages to [flume2storm-users@googlegroups.com](mailto:flume2storm-users@googlegroups.com). Subscription management is done via the [flume2storm discussion group](https://groups.google.com/forum/#!forum/flume2storm-users), as well as consulting the mailing list archives.


## Releases

Flume2Storm can be downloaded from the [release page](https://github.com/Comcast/flume2storm/releases/).

In a Flume2Storm release tarball, there are:

- One directory for each component (including the internal ones such as `core`). Each of these directories contains various jar files: for the compiled version, the source code, the javadoc, ...
- A copy of the `LICENSE` and `NOTICE` files
- A copy of the `README.md` file that introduces the project
- A copy of the `CHANGELOG` file that lists all the versions that have been released and what changed between them.


## License

    Copyright 2014 Comcast Cable Communications Management, LLC
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
    http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.



