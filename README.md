# n5-imglib2 [![Build Status](https://github.com/saalfeldlab/n5-imglib2/actions/workflows/build-main.yml/badge.svg)](https://github.com/saalfeldlab/n5-imglib2/actions/workflows/build-main.yml)

ImgLib2 bindings for N5 containers.  The `N5Utils` class provides static convenience methods to open N5 datasets as cache backed `RandomAccessibleInterval`-s (n-dimensional images in ImgLib2 speech), and to save `RandomAccessibleInterval`-s as N5 datasets.  When opening datasets with block-sizes other than the grid raster, you will see the intersection of grid-cell and block filled with data.  Beyond other things, this makes it compatible with datasets stored by [Jan Funke's n5-bindings for Zarr](https://github.com/zarr-developers/zarr/pull/309), where trailing blocks are overhanging instead of cropped to size.  Datasets are saved with trailing blocks cropped to size.

Beanshell example (which is almost Java, but without types, let Eclipse auto-add them in Java code):

```java
import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.imglib2.*;

img = N5Utils.open(new N5FSReader("/home/saalfelds/example.n5"), "/volumes/raw");
```

Will open the `/volumes/raw` dataset from the filesystem based N5 container (directory) `/home/saalfelds/example.n5` as a lazy cell image, i.e. no data is loaded initially, but as you access pixels, the necessary blocks will be loaded and cached in memory.

See also scripts demonstrating
* [how to read and write imglib2 images with the methods in `N5Utils`](https://github.com/saalfeldlab/n5-imglib2/blob/master/scripts/readProcessWriteDemo.bsh)
* [how to read and write ImageJ images with the methods in `N5IJUtils`](https://github.com/saalfeldlab/n5-ij/blob/master/scripts/readProcessWriteIJDemo.bsh)
