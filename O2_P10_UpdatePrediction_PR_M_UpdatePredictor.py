# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 17:08:22 2020

@author: asheikh
"""

import pandas as pd
import numpy as np
import statistics as st
import calendar
import datetime
import re
import matplotlib.pyplot as plt
from datetime import datetime
pd.options.mode.chained_assignment = None

## Loading Data
PastYearData =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/PastYearDataNew.csv"
    ,encoding='utf-8')
CurrentYearData =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/CurrentYearDataNew.csv"
    ,encoding='utf-8')
SkuAddDate =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/skuAddDate.csv"
    ,encoding='utf-8')
settings =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/Update_Schedule_Setting.csv"
    ,encoding='utf-8')

#Combine Past and recent years sales data
SD = pd.concat([PastYearData, CurrentYearData], ignore_index=True)
SDnum = pd.concat([PastYearData, CurrentYearData], ignore_index=True)

year = int(settings.iloc[0, 1])
updateLimit = int(settings.iloc[1, 1])
monthChange = 1+(settings.iloc[2, 1])
dThresh = int(settings.iloc[3, 1])


# Convert Order_Date string to date and extract relevant date values
#https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
SD['Order_Date'] = pd.to_datetime(SD['Order_Date'], format= "%d-%b-%y")
SD['OD_Year'] = SD['Order_Date'].dt.strftime('%Y')
SD['OD_MonthNum'] = SD['Order_Date'].dt.strftime('%m')
SD['OD_MonthLab'] = SD['Order_Date'].dt.strftime('%B')
SD['OD_MonthDay'] = SD['Order_Date'].dt.strftime('%d')
SD['OD_WeekDay'] = SD['Order_Date'].dt.strftime('%A')
SD["NetRevenue"]= SD.Total_Net_Price_CAD_ - SD.Total_Refunded_CAD_

# Filter out any blank orderdate values
FilterSD =    SD[(SD['Order_Date']!="")]

#Extract meta data from SD
##Identify all unique brands
AllBrands = SD.attribute_set.unique().astype(str)
AllBrands = AllBrands[(AllBrands!="Discontinued") & (AllBrands!="nan") ]
AllSuppliers = SD.Supplier.unique()
AllBucketValues = SD.Order_Bucket.unique()

#Group by Attribute set & year sold, then count number of sales
monthlyGroupingN = (FilterSD.groupby(["attribute_set", "OD_MonthNum", "OD_MonthDay"], as_index=False)
                ['Order_Date'].
                agg({"count":"count"}).sort_values(["attribute_set","OD_MonthNum","OD_MonthDay"] , ascending = True))
monthlyGroupingR = (FilterSD.groupby(["attribute_set", "OD_MonthNum", "OD_MonthDay"], as_index=False)
                ['NetRevenue'].
                agg({"NetRevenue":"sum"}).sort_values(["attribute_set","OD_MonthNum", "OD_MonthDay"] , ascending = True))
monthlyGroupingAll = monthlyGroupingN.merge(monthlyGroupingR, on=('attribute_set', 'OD_MonthNum', "OD_MonthDay")).reset_index(drop=True)


DueDatePrediction = pd.DataFrame(columns = ['attribute_set',"MNum",'Month' , 'Day',
                                                "Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug",
                                                "Sep", "Oct", "Nov", "Dec","TotalOAD","peakDays", "test1"])

for value in range(len(AllBrands)-1):

    brand = AllBrands[value]
    BrandYearSS = monthlyGroupingAll[(monthlyGroupingN['attribute_set']==brand)].reset_index(drop=True)

    BrandYearSS["OverSales"] =  BrandYearSS["count"] - st.mean(BrandYearSS["count"])
    BrandYearSS["OverRev"] = BrandYearSS["NetRevenue"] -  st.mean(BrandYearSS["NetRevenue"])

    OverIdentify = BrandYearSS[(BrandYearSS["OverSales"]>0) & (BrandYearSS["OverRev"]>0)]
    OverIdentify_Tally = OverIdentify.groupby(["OD_MonthNum"], as_index=False)['OD_MonthNum'].agg({"OverCounter":"count"})

    OverIdentify_Final = pd.DataFrame({"OD_MonthNum":["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]})
    OverIdentify_Final = OverIdentify_Final.merge(OverIdentify_Tally, on=('OD_MonthNum'), how = "outer").reset_index(drop=True)
    OverIdentify_Final = OverIdentify_Final.fillna(0)

    if len(OverIdentify_Final)>1:

        for m in range(len(OverIdentify_Final)-1):
            Base = (OverIdentify_Final.loc[m+1, "OverCounter"])
            Threshold = (OverIdentify_Final.loc[m, "OverCounter"]*monthChange)
            if Base>Threshold:
                if Base>=dThresh:###Consider reducing to 6
                    OverIdentify_Final.loc[m, "test1"] = "Update"
                else:
                    OverIdentify_Final.loc[m, "test1"]  = ""
            else:
                OverIdentify_Final.loc[m, "test1"]  = ""

        FindDup = len(OverIdentify_Final[OverIdentify_Final["test1"]=="Update"])

        if FindDup > 0:
            if FindDup==1:
                OverIdentify_Final["test2"] = OverIdentify_Final["test1"]

                WinningMonth = OverIdentify_Final[OverIdentify_Final.test2 == "Update"].reset_index(drop=True).loc[0,"OD_MonthNum"]
                SubsetWinMonth = monthlyGroupingAll[(monthlyGroupingAll.attribute_set== brand) & (monthlyGroupingAll.OD_MonthNum== WinningMonth)].reset_index(drop=True)

            elif FindDup>1:
                MultiUpdate = OverIdentify_Final[OverIdentify_Final["test1"]=="Update"]
                MaxMonths = (len(OverIdentify_Final)-1 - MultiUpdate.index[-1])+1
                MultiUpdate["index"] = MultiUpdate.index
                MultiUpdate = MultiUpdate.reset_index(drop=True)

                for s in range(len(MultiUpdate)):
                    if s != (len(MultiUpdate)-1):
                        IndexVal = MultiUpdate.loc[s,"index"]
                        MultiUpdate.loc[s, "Value"] = sum(OverIdentify_Final.loc[IndexVal+1:(MaxMonths+IndexVal), "OverCounter"])
                    else:
                        IndexVal = MultiUpdate.loc[s,"index"]
                        MultiUpdate.loc[s, "Value"] = sum(OverIdentify_Final.loc[IndexVal+1:(MaxMonths+IndexVal-1), "OverCounter"], OverIdentify_Final.loc[0, "OverCounter"])

                WinMonth = MultiUpdate.sort_values("Value", ascending = False).reset_index(drop=True).loc[0,"index"]

                for m in range(len(OverIdentify_Final)-1):
                    if m == WinMonth:
                        OverIdentify_Final.loc[WinMonth, "test2"] = "Update"
                    else:
                        OverIdentify_Final.loc[m, "test2"]  = ""

                WinningMonth = OverIdentify_Final[OverIdentify_Final.test2 == "Update"].reset_index(drop=True).loc[0,"OD_MonthNum"]
                SubsetWinMonth = monthlyGroupingAll[(monthlyGroupingAll.attribute_set== brand) & (monthlyGroupingAll.OD_MonthNum== WinningMonth)].reset_index(drop=True)


            #Determine the end of peak
            count = 0
            sumDays = 0
            dayCount = 0

            for i in range(int(WinningMonth), len(OverIdentify_Final)):
                if i != 12:
                    month = OverIdentify_Final.loc[i, "OD_MonthNum"]
                    days = OverIdentify_Final.loc[i, "OverCounter"]
                    count = count + 1
                    sumDays = sumDays + days
                    avg = (sumDays/count)*.75

                    if(days >= avg):
                        dayCount = dayCount + 30
                    else:
                        break
                elif i == 12:
                    month = OverIdentify_Final.loc[0, "OD_MonthNum"]
                    days = OverIdentify_Final.loc[0, "OverCounter"]
                    count = count + 1
                    sumDays = sumDays + days
                    avg = (sumDays/count)*.75

                    if(days >= avg):
                        dayCount = dayCount + 30
                    else:
                        break


            #Determine day of WinningMonth of due date
            TextMonth = calendar.month_name[int(WinningMonth)]
            MaxCountIndex = SubsetWinMonth[SubsetWinMonth["count"] == (SubsetWinMonth["count"].max())].index[0]
            MaxRevIndex = SubsetWinMonth[SubsetWinMonth["NetRevenue"] == (SubsetWinMonth["NetRevenue"].max())].index[0]

            if MaxCountIndex == MaxRevIndex:
                MaxOfMonth = MaxCountIndex
                if MaxOfMonth == len(SubsetWinMonth)-1:
                    MinusLast = SubsetWinMonth.loc[0:len(SubsetWinMonth)-2]
                    MaxCountIndex = MinusLast[MinusLast["count"] == (MinusLast["count"].max())].index[0]
                    MaxRevIndex = MinusLast[MinusLast["NetRevenue"] == (MinusLast["NetRevenue"].max())].index[0]
                    if MaxCountIndex == MaxRevIndex:
                        MaxOfMonth = MaxCountIndex
                        MinSubset = SubsetWinMonth.loc[MaxOfMonth:]
                    else:
                        MaxOfMonth = min(MaxCountIndex, MaxRevIndex)
                        MinSubset = SubsetWinMonth.loc[MaxOfMonth:]
                else:
                    MinSubset = SubsetWinMonth.loc[MaxOfMonth:]
            else:
                MaxOfMonth = min(MaxCountIndex, MaxRevIndex)
                MinSubset = SubsetWinMonth.loc[MaxOfMonth:]

            MinCountIndex = MinSubset[MinSubset["count"]==MinSubset.loc[MaxOfMonth:, "count"].min()].index[-1]
            MinRevIndex =   MinSubset[MinSubset["NetRevenue"]==MinSubset.loc[MaxOfMonth:, "NetRevenue"].min()].index[-1]

            if MinCountIndex == MinRevIndex:
                FinalIndex = MinRevIndex
                SubsetWinMonth.loc[FinalIndex,"DeadLine"] = "***"
                WinDay = SubsetWinMonth.loc[FinalIndex, "OD_MonthDay"]
            else:
                FinalIndex = min(MinCountIndex, MinRevIndex)
                SubsetWinMonth.loc[FinalIndex,"DeadLine"] = "***"
                WinDay = SubsetWinMonth.loc[FinalIndex, "OD_MonthDay"]
        else:
            TextMonth = "None"
            WinningMonth = 0
            WinDay = 0

    #Formatting Data
    AllRecomMon = str()
    AllRecomMonRou = OverIdentify_Final[OverIdentify_Final["test1"]=="Update"].reset_index(drop = True)
    for m in range(len(AllRecomMonRou)):
        AllRecomMon = AllRecomMon + calendar.month_name[int(AllRecomMonRou.loc[m,"OD_MonthNum"])]+ "-"

    OC = OverIdentify_Final["OverCounter"]
    TotalOAD = sum(OC)
    DueDateData = brand,WinningMonth,TextMonth,WinDay,OC[0],OC[1],OC[2],OC[3],OC[4],OC[5],OC[6],OC[7],OC[8],OC[9],OC[10],OC[11],TotalOAD,dayCount, str(AllRecomMon)

    DueDatePrediction = DueDatePrediction.append(pd.Series(DueDateData, index=DueDatePrediction.columns),  ignore_index=True)
    Daydata = DueDatePrediction[["attribute_set","peakDays"]]

#subset internal sku, attribute set and order date from SD data frame
SoldSkus = pd.DataFrame(SD.internal_sku.unique().astype(str))
SoldSkus.columns = ["internal_sku"]

#Merge SoldSkus with SkuAddDate
SoldSkus = pd.merge(SoldSkus, SkuAddDate, how='inner', on="internal_sku")
SoldSkus['add_date'] = pd.to_datetime(SoldSkus['add_date'], format= "%Y-%m-%d")
BrandLabel = SD[["internal_sku", "attribute_set"]]
SoldSkus = pd.merge(SoldSkus, BrandLabel, how='inner', on="internal_sku").drop_duplicates(keep=False)
SoldSkus= SoldSkus.reset_index()

for i in range(len(SoldSkus)):
    sku = SoldSkus.loc[i,"internal_sku"]
    Brand = SoldSkus.loc[i,"attribute_set"]
    AddDate = SoldSkus.loc[i,"add_date"]

    AllOrderDates = SD[SD["internal_sku"]==sku].Order_Date.sort_values().reset_index().loc[0,"Order_Date"]
    PeakDayValue = Daydata[Daydata["attribute_set"]==Brand].reset_index().loc[0, "peakDays"]

    RevSubset = SD[SD["internal_sku"]==sku][["Order_Date", "NetRevenue" ]]
    RevSubset["SellDistance"] = abs(RevSubset["Order_Date"]-AddDate).dt.days
    RevSubset["SellDifference"] = PeakDayValue -  RevSubset["SellDistance"]
    NewSkuRev = sum(RevSubset[RevSubset["SellDifference"] >= 0]["NetRevenue"])

    SoldSkus.loc[i,"FirstSoldDate"] = AllOrderDates
    SoldSkus.loc[i,"PeakDayRange"] = PeakDayValue
    SoldSkus.loc[i,"NewSkuRev"] = NewSkuRev

    print(SoldSkus.loc[i,"internal_sku"], "         ", NewSkuRev)

    sData = str(round((i/len(SoldSkus))*100)), " ", str(SoldSkus.loc[i,"internal_sku"]), " ", str(NewSkuRev)


#if GlobalStatus == "normal":
SoldSkus['DayDiff'] = abs(SoldSkus.FirstSoldDate-SoldSkus.add_date).dt.days


for i in range(len(SoldSkus)):
    DayDiff = SoldSkus.loc[i,"DayDiff"]
    pdr = SoldSkus.loc[i,"PeakDayRange"]

    if DayDiff <= pdr:
        SoldSkus.loc[i, "UnderPeakDays"] = 1
    else:
        SoldSkus.loc[i, "UnderPeakDays"] = 0

    if (DayDiff<365):
        SoldSkus.loc[i, "UnderYear"] = 1
        SoldSkus.loc[i, "OverYear"] = 0

        if(DayDiff<180):
            SoldSkus.loc[i, "HalfYear"] = 1
        else:
            SoldSkus.loc[i, "HalfYear"] = 0
    else:
        SoldSkus.loc[i, "UnderYear"] = 0
        SoldSkus.loc[i, "OverYear"] = 1


AvgDays = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['DayDiff'].
                agg({"AvgDayDiff":"mean"}).sort_values(["AvgDayDiff"] , ascending = True))

TimesSold = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['DayDiff'].
                agg({"TimesSold":"count"}).sort_values(["TimesSold"] , ascending = True))

UnderYear = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['UnderYear'].
                agg({"UnderYear":"sum"}))

OverYear = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['OverYear'].
                agg({"OverYear":"sum"}))

HalfYear = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['HalfYear'].
                agg({"HalfYear":"sum"}))

UnderPeakDays = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['UnderPeakDays'].
                agg({"UnderPeakDays":"sum"}))

NewSkuRev = (SoldSkus.groupby(["attribute_set"], as_index=False)
                ['NewSkuRev'].
                agg({"NewSkuRev":"sum"}))

finalskuCount = AvgDays.merge(TimesSold, on="attribute_set" )
finalskuCount = finalskuCount.merge(UnderYear, on="attribute_set" )
finalskuCount = finalskuCount.merge(OverYear, on="attribute_set" )
finalskuCount = finalskuCount.merge(HalfYear, on="attribute_set" )
finalskuCount = finalskuCount.merge(UnderPeakDays, on="attribute_set" )
finalskuCount = finalskuCount.merge(NewSkuRev, on="attribute_set" )
finalskuCount["%UnderYear"] = finalskuCount["UnderYear"] /(finalskuCount["OverYear"] + finalskuCount["UnderYear"])
finalskuCount["%HalfYear"] = finalskuCount["HalfYear"] /(finalskuCount["OverYear"] + finalskuCount["UnderYear"])
finalskuCount["%UnderPeakDays"] = finalskuCount["UnderPeakDays"] /(finalskuCount["OverYear"] + finalskuCount["UnderYear"])
finalskuCount = finalskuCount.sort_values(["TimesSold", "AvgDayDiff", "UnderYear"] , ascending = [False,True, True])

#Aquire net revenue for all brands
BrandRev = monthlyGroupingAll.groupby("attribute_set")["NetRevenue"].agg("sum")
FinalBrandProfile = finalskuCount.merge(BrandRev, on="attribute_set" ).merge(DueDatePrediction, on="attribute_set" )
FinalBrandProfile["%NewSkuRev"] = (FinalBrandProfile["NewSkuRev"]/FinalBrandProfile["NetRevenue"])*100
FinalBrandProfile["%NewSkuRev"] = (FinalBrandProfile["NewSkuRev"]/FinalBrandProfile["NetRevenue"])*100

UpdatePrioritydf = FinalBrandProfile[["attribute_set", "MNum", "Day", "NewSkuRev", "%NewSkuRev"]]
UpdatePrioritydf["MNum"] = pd.to_numeric(UpdatePrioritydf["MNum"])
UpdatePrioritydf["Day"] = pd.to_numeric(UpdatePrioritydf["Day"])
UpdatePrioritydf = UpdatePrioritydf[(UpdatePrioritydf["NewSkuRev"]>0) & (UpdatePrioritydf["MNum"] != 0)]

UpdatePrioritydf = UpdatePrioritydf.sort_values(["MNum", "Day", "NewSkuRev"], ascending = [True, True, False]).reset_index()
#UpdatePrioritydf.to_csv (r'\\192.168.2.32\Group\Data Team\Abul\3. Final Folder\UpdatePrioritydf.csv', index = None, header=True)

UpdatePrioritydf.loc[(UpdatePrioritydf["MNum"]==2) & (UpdatePrioritydf["Day"]==29), "Day"] = 28
UpdatePrioritydf["Year"] = year
UpdatePrioritydf["MNum"] = UpdatePrioritydf["MNum"].map("{:02}".format)
UpdatePrioritydf["Day"] = UpdatePrioritydf["Day"].map("{:02}".format)

UpdatePrioritydf["Year"] = UpdatePrioritydf["Year"].astype(str)
UpdatePrioritydf["MNum"] = UpdatePrioritydf["MNum"].astype(str)
UpdatePrioritydf["Day"] = UpdatePrioritydf["Day"].astype(str)

UpdatePrioritydf["Date"] = UpdatePrioritydf["Year"]+"-"+UpdatePrioritydf["MNum"]+"-"+UpdatePrioritydf["Day"]
UpdatePrioritydf["Date"] = pd.to_datetime(UpdatePrioritydf["Date"], format= "%Y-%m-%d")
UpdatePrioritydf["Weekday"] = UpdatePrioritydf["Date"].dt.dayofweek
UpdatePrioritydf["WeekNum"] = UpdatePrioritydf["Date"].dt.week
UpdatePrioritydf["Ref"] = 0

for w in reversed(range(53)):
    weekBrands = UpdatePrioritydf[UpdatePrioritydf["WeekNum"]==w]
    BrandPerWeek = len(weekBrands)
    #print(BrandPerWeek)
    if(BrandPerWeek > updateLimit):
        BrandDiff = BrandPerWeek - updateLimit
        MovingBrandList = (list(weekBrands.sort_values("NewSkuRev", ascending = True)
                                .reset_index(drop=True)[0:BrandDiff]["attribute_set"]))
        UpdatePrioritydf.loc[UpdatePrioritydf["attribute_set"].isin(MovingBrandList), "WeekNum"] = w-1
    weekBrands = UpdatePrioritydf[UpdatePrioritydf["WeekNum"]==w].sort_values("NewSkuRev", ascending = False).reset_index()
    for f in range(len(weekBrands)):
        brand = weekBrands["attribute_set"][f]
        UpdatePrioritydf.loc[UpdatePrioritydf["attribute_set"]==brand, "Ref"] = f

UpdatePrioritydf["WeekNum"] = UpdatePrioritydf["WeekNum"].map("{:02}".format)

UpdateSchedule = pd.DataFrame(data={'Day': 1, 'WeekNum': range(0,53), 'Year': year})
UpdateSchedule["Day"] = UpdateSchedule["Day"].astype(str)
UpdateSchedule["WeekNum"] = UpdateSchedule["WeekNum"].map("{:02}".format).astype(str)
UpdateSchedule["Year"] = UpdateSchedule["Year"].astype(str)

UpdateSchedule["Date_temp"] = UpdateSchedule["Day"]+"-"+UpdateSchedule["WeekNum"]+"-"+UpdateSchedule["Year"]
UpdateSchedule["Date_temp"] = pd.to_datetime(UpdateSchedule["Date_temp"], format= "%w-%W-%Y")
UpdateSchedule["Month"] = UpdateSchedule["Date_temp"].dt.month_name()
UpdateSchedule["Date"] = UpdateSchedule["Date_temp"].dt.day

RankedBrands = UpdatePrioritydf.pivot(index='WeekNum', columns='Ref', values='attribute_set')

UpdateScheduleFinal = (UpdateSchedule
                       .merge(RankedBrands, on=('WeekNum'))
                       .drop(columns=["Day","WeekNum","Year","Date_temp"])
                       .fillna(""))

date = str(datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))

(UpdateScheduleFinal.to_csv(
    ("//192.168.2.32/Group/Data Team/Restricted_Data_Sources/2_Output_Folder/UpdateSchedulePrediction"
    +"-"+str(year)+"-"+str(updateLimit)+"-"+str(monthChange)+"-"+str(dThresh)+".csv")
    ,index = None, header=True))





