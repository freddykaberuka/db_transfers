const mysql = require('mysql');

const config = [
  {
    sourceConfig: {
      databases: ['testing_source'],
      tables: ['User'],
      columns: [['UserID', 'FirstName', 'Address']]
    },
    destinationConfig: {
      databases: 'testing_destination',
      tables: ['Users'],
      columns: [['user_id', 'names', 'address']]
    }
  },
  {
    sourceConfig: {
      databases: ['database2'],
      tables: ['Crop'],
      columns: [['crop_id', 'crop_Name']]
    },
    destinationConfig: {
      databases: 'testing_destination',
      tables: ['famer'],
      columns: [['famerID', 'LastName']]
    }
  }
];

const transportData = (config) => {
  const { sourceConfig, destinationConfig } = config;
  const databases = sourceConfig.databases.map((databaseName, index) => {
    return {
      name: databaseName,
      tables: [
        {
          name: sourceConfig.tables[index],
          columns: sourceConfig.columns[index]
        }
      ]
    };
  });

  const tableMappings = sourceConfig.databases.map((databaseName, index) => {
    return {
      sourceDatabaseIndex: index,
      sourceTableIndex: 0,
      destinationTable: destinationConfig.tables[index],
      fieldMappings: sourceConfig.columns[index].map((column, columnIndex) => {
        return {
          sourceFieldIndex: columnIndex,
          destinationField: destinationConfig.columns[index][columnIndex]
        };
      })
    };
  });

  const processNextDatabase = (databaseIndex) => {
    if (databaseIndex >= sourceConfig.databases.length) {
      console.log('Data transfer completed for all databases.');
      return;
    }

    const database = sourceConfig.databases[databaseIndex];
    const sourceConnection = mysql.createConnection({
      host: 'localhost',
      user: 'freddy',
      password: 'password',
      database: database
    });

    sourceConnection.connect((err) => {
      if (err) {
        console.error(`Error connecting to source database ${databaseIndex + 1}:`, err);
        processNextDatabase(databaseIndex + 1);
        return;
      }
      console.log(`Connected to source database ${databaseIndex + 1}.`);

      const destinationConnection = mysql.createConnection({
        host: 'localhost',
        user: 'freddy',
        password: 'password',
        database: destinationConfig.databases
      });

      destinationConnection.connect((err) => {
        if (err) {
          console.error('Error connecting to destination database:', err);
          sourceConnection.end();
          processNextDatabase(databaseIndex + 1);
          return;
        }
        console.log('Connected to destination database.');

        const tableMapping = tableMappings.find((tableMapping) => {
          return (
            tableMapping.sourceDatabaseIndex === databaseIndex &&
            tableMapping.sourceTableIndex === 0
          );
        });

        if (!tableMapping) {
          console.error(`Table mapping not found for source database ${databaseIndex + 1}.`);
          sourceConnection.end();
          destinationConnection.end();
          processNextDatabase(databaseIndex + 1);
          return;
        }

        const sourceTable = databases.find((database) => database.name === sourceConfig.databases[databaseIndex]).tables[0].name;
        const sourceColumns = databases.find((database) => database.name === sourceConfig.databases[databaseIndex]).tables[0].columns.join(', ');

        const sourceQuery = `SELECT ${sourceColumns} FROM ${database}.${sourceTable}`;
        sourceConnection.query(sourceQuery, (err, rows) => {
          if (err) {
            console.error(`Error retrieving data from source ${sourceTable} in database ${database}:`, err);
            sourceConnection.end();
            destinationConnection.end();
            processNextDatabase(databaseIndex + 1);
            return;
          }

          const columnTypes = rows.reduce((types, row) => {
            types[row.Field] = row.Type;
            return types;
          }, {});

          const mappedRows = rows.map((row) => {
            const mappedRow = {};

            tableMapping.fieldMappings.forEach(({ sourceFieldIndex, destinationField }) => {
              const sourceColumn = databases.find((database) => database.name === sourceConfig.databases[databaseIndex]).tables[0].columns[sourceFieldIndex];
              const sourceType = columnTypes[sourceColumn];

              mappedRow[destinationField] = row[sourceColumn];
            });

            return mappedRow;
          });

          const insertData = `INSERT INTO ${destinationConfig.databases}.${tableMapping.destinationTable} SET ?`;

          const insertRow = (index) => {
            if (index >= mappedRows.length) {
              console.log(`Successfully transferred data from source ${sourceTable} in database ${database} to destination table ${tableMapping.destinationTable} in database ${destinationConfig.databases}.`);
              destinationConnection.end();
              sourceConnection.end();
              processNextDatabase(databaseIndex + 1);
              return;
            }

            const mappedRow = mappedRows[index];

            destinationConnection.query(insertData, mappedRow, (err, result) => {
              if (err) {
                console.error(`Error inserting data into destination ${tableMapping.destinationTable} in database ${destinationConfig.databases}:`, err);
              } else {
                console.log(`Inserted row ${index + 1} of ${mappedRows.length} into destination ${tableMapping.destinationTable} in database ${destinationConfig.databases}.`);
              }

              insertRow(index + 1);
            });
          };

          insertRow(0);
        });
      });
    });
  };

  processNextDatabase(0);
};

transportData(config[0]);
transportData(config[1]);
