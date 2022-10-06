# DataQuality - Validate your data everyday when it is loaded

A approach to validate the data stored in your Databases/Warehouse. 


### Getting started
To run DataQuality you must first define 1 files.

```yaml
processName: df1
schemaName: indian_premier_league
tableName: ipl_matches

validationRules:
    - validationGranualityLevel: table
      isRuleActive: true
      totalCountGte: 100
      errorLevel: error/warn
      filterCondition: [date='2017-04-05',team1='Sunrisers Hyderabad']

    - validationGranualityLevel: column
      columnName: city
      isRuleActive: true
      completeness: 99
      

    - validationGranualityLevel: column
      columnName: XYZ
      isRuleActive: false
      totalCountGte: SOMEVALUE
      totalCountLte: SOMEVALUE
      totalCountEquals: SOMEVALUE
      totalCountBtw: SOMEVALUE
      completeness: SOMEVALUE
      uniqueness: SOMEVALUE
      hasSelectiveValue: SOMEVALUE
      hasMin: SOMEVALUE
      hasMax: SOMEVALUE
      nonNegative: SOMEVALUE
      withinRange: SOMEVALUE
      customSql: Mention Your sql
      errorLevel: error/warn
      filterCondition: [team1='Sunrisers Hyderabad']
```

## Roadmap

### Sql Validation
- completeness
- totalCountGte
- totalCountLte
- totalCountEquals
- totalCountBtw
- uniqueness
- hasSelectiveValue 
- hasMin
- hasMax
- nonNegative
- withinRange
- customSql

### Other
- Expand the validation on various other databases like.
    - MySql
    - MongoDB
    - SnowFlake


- Currently output is stored as a Hive Dataset. Would like to make it configurable for System
    - File System
    - Kafka Topic
    - Some RDBMS








