# CONcurrent DOWnloads from AWS S3

**WARNING! Not yet for production usage**

Download speed from S3 can be significantly improved by
downloading parts of the file concurrently. This crate
does exactly that.

Unlike e.g. the AWS Java SDK this library does not download 
the parts as upladed but ranges.

## License

condow is distributed under the terms of both the MIT license and the Apache License (Version 2.0).

See LICENSE-APACHE and LICENSE-MIT for details.

License: Apache-2.0/MIT