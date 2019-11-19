# seaflow-transfer

seaflow-transfer is a tool to transfer SeaFlow SFL and EVT files.
Source and destination can be locations in a local filesystem or an SFPT server.
Every time the tool is run, all SFL files and only new EVT files are transferred.
EVT files will be gzipped if necessary at the destination.
This tool tries to ensure that the data at the destination is always in a form that is safe for analysis.
To this end:

* the most recent EVT file is never transferred as it may be incomplete
* files at the destination are first written to a temporary file which is only renamed upon successful transfer

These two features mean that if an EVT file is visible with the correct path at the destination, it is ready to be analyzed.

However, it is possible that the last line in the most recent SFL file may be in an incomplete state and will only be corrected on the next transfer.
Any tool reading this SFL file should be prepared to handle a malformed final line.

## Installation

Either download a binary from the releases section of this github repo, or run

`go get https://github.com/armbrustlab/seaflow-transfer/cmd/seaflow-transfer`

## Usage

Run `seaflow-transfer -help` for CLI usage.