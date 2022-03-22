import urllib.request
from datetime import date


def geturlByYearNMonth(year,month,type):
    if(month<10):
        urlstring='https://s3.amazonaws.com/nyc-tlc/trip+data/'+type+'_tripdata_'+str(year)+'-0'+str(month)+'.csv' 
    else:
        urlstring='https://s3.amazonaws.com/nyc-tlc/trip+data/'+type+'_tripdata_'+str(year)+'-'+str(month)+'.csv' 
    return(urlstring)

def downlodFileFromURL(download_url,fileName):
    print(download_url)
    print(fileName)
    response = urllib.request.urlopen(download_url)    
    file = open('.\\NYCfiles\\'+fileName + ".csv", 'wb')
    file.write(response.read())
    file.close()
    

if __name__ == "__main__":
    range_of_years= range(date.today().year-2,date.today().year)
    for yearData in range_of_years:
        for month_of_year in range(1,13):
            for cabType in ('green','yellow'):
                url=geturlByYearNMonth(yearData,month_of_year,cabType)
                fileName=cabType+'_tripdata_'+str(yearData)+'-'+str(month_of_year)
                downlodFileFromURL(url,fileName)
      