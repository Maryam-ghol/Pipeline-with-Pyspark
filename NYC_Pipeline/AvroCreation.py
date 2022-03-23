import avro.schema
from avro.datafile import  DataFileWriter
from avro.io import  DatumWriter
import csv
from collections import namedtuple
from datetime import date
from os.path import exists


def read_NYC_data(path):
    with open(path, 'r') as data:
        fieldsT=[]
        fieldsT=data.readline()
        NYCRecord = namedtuple('NYCRecord', fieldsT)
        reader = csv.reader(data, delimiter = ",")
        for row in map(NYCRecord._make, reader):
            yield row


def parse_schema(path):
    with open(path, 'r') as data:
        return avro.schema.parse(data.read())


def serialize_records(records, outpath,schemaPath):
    schema = parse_schema(schemaPath)
    with open(outpath, 'w') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record = dict((f, getattr(record, f)) for f in record._fields)
            print(record)
            writer.append(record)
        writer.close()


if __name__ == "__main__":
    rangeYears= range(date.today().year-3,date.today().year)
    cabTypes=['green']
    for yearData in rangeYears:
        for monthYear in range(1,13):
            for cabType in cabTypes:
                fileName=cabType+'_tripdata_'+str(yearData)+'-'+str(monthYear)
                csvpath='.\\NYCfiles\\'+fileName+'.csv'
                outFile='.\\NYCfiles\\'+fileName+'.avro'
                schemaPath='NYC'+cabType+'.avsc'
                if(exists(csvpath)):
                    serialize_records( read_NYC_data(csvpath),outFile,schemaPath)
                    
              