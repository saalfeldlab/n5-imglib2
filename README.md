# n5-imglib2

ImgLib2 bindings for N5 containers.  The `N5Utils` class provides static convenience methods to open and save N5 datasets.  By Beanshell example (which is almost Java, but without types, let Eclipse auto-add them in Java code):

```java
import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.imglib2.*;

img = N5Utils.open(new N5FSReader("/home/saalfelds/example.n5"), "/volumes/raw");
```

Will open the `/volumes/raw` dataset from the filesystem based N5 container (directory) `/home/saalfelds/example.n5` as a lazy cell image, i.e. no data is loaded initially, but as you access pixels, the necessary blocks will be loaded and cached in memory.

Please add more examples as required.
