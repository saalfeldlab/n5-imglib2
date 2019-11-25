# Axes lookup datastructure 

```
\
├── ".axes"
│   ├── "some2DVector"
│   │   ├── attributes.json {"dataType":"int32","compression":{"type":"raw"},"blockSize":[2,2],"dimensions":[2,2]}
│   ├── "a3DVector"
│   │   ├── attributes.json {"dataType":"int32","compression":{"type":"raw"},"blockSize":[2,2,2],"dimensions":[2,2,2]}
│   ┊
│
├── ...
┊
```

Each "vector" dataset is a 2^n size single block dataset that enumerates the axes indices.  The 2D axes lookup for ImgLib2 vectors (F-order) looks like this:

```
-1  0  ->  -1 0 1 -1
 1 -1
```

The 3D axes lookup for numpy vectors (C-order) looks like this:

```
-1  2  ->  -1 2 1 -1 0 -1 -1 -1
 1 -1

 0 -1
-1 -1
```

Tensor and vector aware API can then load the index lookup by loading the dataset as a tensor and access the first positive coordinate at each axis according to the API's axes indexing scheme.

This creates a named permutation for vectors.

The ImgLib2 bindings offer API methods to create and use such lookups.  Naturally, the mapping is defined as coming from ImgLib2 F-order:

```java
createAxes(
  String axesName,
  int[] axes);

readDoubleVectorAttribute(
  N5Reader n5,
  String groupName,
  String attributeName,
  int[] axes); 
writeDoubleVectorAttribute(
  double[] vector,
  N5Writer n5,
  String groupName,
  String attributeName,
  int[] axes);
readLongVectorAttribute(
  N5Reader n5,
  String groupName,
  String attributeName,
  int[] axes); 
writeLongVectorAttribute(
  double[] vector,
  N5Writer n5,
  String groupName,
  String attributeName,
  int[] axes);
```






