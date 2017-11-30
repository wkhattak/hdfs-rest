# Library: HDFS REST
## Overview   
  
A Java library for getting the 5 min consumption & forecast data from HDFS and then writing out as csv files.

## Requirements
See [POM file](./pom.xml)


## Usage Example
```java
java -cp C:\Users\Admin\Documents\EclipseProjects\hdfs-rest\target\hdfs-rest-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.khattak.bigdata.filesystems.hdfs.HDFSRest 192.168.70.134:50070 /hadoop/data/smartmeter/statistics/city_usage/fifteen_minute/2016/10/1/fifteen_minute_statistics.csv
```

## License
The content of this project is licensed under the [Creative Commons Attribution 3.0 license](https://creativecommons.org/licenses/by/3.0/us/deed.en_US).