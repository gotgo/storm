# Readme
This folder contains go code generated by thrift.  It has been manually edited to correct the namespaces.  
If it is regenerated the following steps need to be taken:

## Manual Changes

* Move the generated folder that contains the topmost go code out of the deeply nested generated location to a more reasonable path.
* Move all the files in the package "storm", into their own folder named "topology" and then change the package name to from 'storm' to 'topology'
* Change the file references in the other applications to correctly point to this new storm folder
> using storm "github.com/gotgo/storm/thrift/topology"


## Regenerate

```
thrift -r --gen go storm.thrift
```

## Thrift

https://thrift.apache.org/lib/go


