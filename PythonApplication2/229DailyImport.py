
import pandas as pd
import pyodbc
import sqlalchemy
import math
import datetime
import numpy as np
import os.path as path

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-E7J4R0R;'
                      'Database=example;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
now = datetime.date.today() # - datetime.timedelta(days=3)
today = str(now)[:10]
#today = '2019-09-06'
wh = pd.DataFrame()
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\wh.xlsx"):
    wh = pd.read_excel("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\wh.xlsx")
print('Start')

### 1. ItemList ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\itemlist.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("ItemList")

    itemlist = pd.ExcelFile("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\itemlist.xls")
    sheet_num = len(itemlist.sheet_names)
    print('Read ItemList')
    
    sql = "delete itemlist"
    cursor.execute(sql)
    conn.commit()
    print('Delete ItemList')

    for numofsheet in range(0, sheet_num):
        for index, row in itemlist.parse(numofsheet).iterrows():
            brandname = row['Brand Name']
            if pd.isnull(brandname):
                brandname = ''
            if pd.isnull(row['Item Type']) or row['Item Type'] != "Countable" or row['Saleable'] != 1:
                continue
            cursor.execute(
                "INSERT INTO ItemList (Category, CategoryID, ItemCode, BrandName, Phaseout, Level) VALUES (?,?,?,?,?,?)",
                row['TopCategory'], row['TopCategoryId'], row['Item Code'], brandname, row['Phase Out'], row['Level'])
            cursor.execute("INSERT INTO ItemLevel ([itemcode],[date],[level]) VALUES (?,?,?)", row['Item Code'], today, row['Level'])
            conn.commit()
    print('Write ItemList')
    
    sql = "update category set category=t1.category from category t2, \
    (select category, categoryid from itemlist group by category, categoryid) t1 \
    where t1.categoryid=t2.categoryid"
    cursor.execute(sql)
    conn.commit()
    print('Update category')

    sql = "update noautopocategory set category=t1.category from noautopocategory t2, \
    (select category, categoryid from itemlist group by category, categoryid) t1 \
    where t1.categoryid=t2.categoryid"
    cursor.execute(sql)
    conn.commit()
    print('Update noautopocategory')

    sql = "update ExcludedBrand set category=t1.category from ExcludedBrand t2, \
    (select category, categoryid from itemlist group by category, categoryid) t1 \
    where t1.categoryid=t2.categoryid"
    cursor.execute(sql)
    conn.commit()
    print('Update ExcludedBrand')

    sql = "select t1.category, t1.categoryid, t2.category, t2.categoryid from itemlist t1 \
    full outer join (select category, categoryid from category union select category, categoryid from NoAutoPOCategory)\
     t2 on t1.categoryid=t2.categoryid where t1.category is null or t2.Category is null group by t1.category, \
     t1.categoryid, t2.Category, t2.categoryid"
    cursor.execute(sql)
    row = cursor.fetchall()
    record = pd.DataFrame(row)
    print("New Category")
    print(record)

#### 8. ExportItemStock ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ExportItemStock.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("ExportItemStock")

    otb = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ExportItemStock.xls")
    sheet_num = len(otb.sheet_names)
    print('Read ExportItemStock')

    cursor.execute("Delete Halifax; Delete HalifaxNP1; Delete HalifaxNP2; Delete HalifaxP1; Delete HalifaxP2;")
    conn.commit()
    print('Delete Halifax')

    for numofsheet in range(0, sheet_num):
        for index, row in otb.parse(numofsheet).iterrows():

            sql = 'INSERT INTO halifax ([ItemCode], [WareHouse], [AvgCost], [OH], [IC], [DI], [TI], \
            [ITI], [SA]) VALUES (?,?,?,?,?,?,?,?,?)'
            cursor.execute(sql, row['ItemCode'], row['WareHouse'], row['AvgCost'], row['OH'], row['IC'], row['DI'], 
                           row['TI'], row['ITI'], row['SA'])
            conn.commit()
    
    sql = "Insert into halifaxNP1 select * from (select t1.category, t1.sku sku1, t1.oh oh1,t1.iti iti1,t1.ti ti1,t1.[PO(di+ic)] 'PO1(di+ic)', \
            case when t2.sku is null then 0 else t2.sku end sku2,\
            case when t2.oh is null then 0 else t2.oh end oh2, \
            case when t2.iti is null then 0 else t2.iti end iti2,\
            case when t2.ti is null then 0 else t2.ti end ti2,\
            case when t2.[PO(di+ic)] is null then 0 else t2.[PO(di+ic)] end 'PO2(di+ic)'\
            from\
            (\
	            select category, count(*) sku, sum(oh) 'oh', sum(iti) iti, sum(ti) ti, sum(PO) 'PO(di+ic)' from\
	            (\
		            select t2.category, t1.itemcode, oh*AvgCost oh, iti*avgcost iti, ti*AvgCost ti, (di+ic)*AvgCost PO from halifax t1, itemlist t2 \
		            where WareHouse like '%markham%' and t1.itemcode=t2.itemcode and t2.phaseout=0 \
		            and (oh!=0 or ti!=0 or iti!=0 or di!=0 or ic!=0) and AvgCost!=0\
	            ) t1\
	            group by category\
            ) t1\
            full outer join\
            (\
	            select category, count(*) sku, sum(oh) 'oh', sum(iti) iti, sum(ti) ti, sum(PO) 'PO(di+ic)'  from\
	            (\
		            select t2.category, t1.itemcode, oh*AvgCost oh, iti*avgcost iti,ti*AvgCost ti, (di+ic)*AvgCost PO from halifax t1, itemlist t2 \
		            where WareHouse like '%North%' and t1.itemcode=t2.itemcode and t2.phaseout=0 \
		            and (oh!=0 or ti!=0 or iti!=0 or di!=0 or ic!=0) and AvgCost!=0\
	            ) t1\
	            group by category\
            ) t2 on t1.category=t2.category)t1\
            order by t1.category"
    cursor.execute(sql)
    conn.commit()
    
    sql = "DECLARE @WH1 VARCHAR(10)\
            DECLARE @WH2 VARCHAR(10)\
            SET @WH1 = '124%'\
            SET @WH2 = '148%'\
            Insert into halifaxNP2 select * from\
            (\
            select category,itemcode,sum(oh1) oh1, sum(sa1) sa1, sum(ti1) ti1, sum(iti1) iti1, sum(di1) di1, sum(ic1) ic1,\
            sum(oh2) oh2, sum(sa2) sa2, sum(ti2) ti2, sum(iti2) iti2, sum(di2) di2, sum(ic2) ic2, avgcost from\
            (select t2.category, t1.itemcode, \
            case when warehouse like @WH1 then t1.oh else 0 end oh1,\
            case when warehouse like @WH1 then t1.sa else 0 end sa1,\
            case when warehouse like @WH1 then t1.ti else 0 end ti1,\
            case when warehouse like @WH1 then t1.iti else 0 end iti1,\
            case when warehouse like @WH1 then t1.di else 0 end di1,\
            case when warehouse like @WH1 then t1.ic else 0 end ic1,\
            case when warehouse like @WH2 then t1.oh else 0 end oh2,\
            case when warehouse like @WH2 then t1.sa else 0 end sa2,\
            case when warehouse like @WH2 then t1.ti else 0 end ti2,\
            case when warehouse like @WH2 then t1.iti else 0 end iti2,\
            case when warehouse like @WH2 then t1.di else 0 end di2,\
            case when warehouse like @WH2 then t1.ic else 0 end ic2, \
            t1.avgcost \
            from halifax t1, itemlist t2\
            where t1.itemcode = t2.itemcode and t2.phaseout=0 \
            and (oh!=0 or sa!=0 or ti!=0 or iti!=0 or di!=0 or ic!=0))t1\
            group by category,itemcode,avgcost)t2\
            order by category, itemcode\
            "
    cursor.execute(sql)
    conn.commit()
    
    sql = "Insert into halifaxP1 select * from (select t1.category, t1.sku sku1, t1.oh oh1,t1.iti iti1,t1.ti ti1,t1.[PO(di+ic)] 'PO1(di+ic)', \
            case when t2.sku is null then 0 else t2.sku end sku2,\
            case when t2.oh is null then 0 else t2.oh end oh2, \
            case when t2.iti is null then 0 else t2.iti end iti2,\
            case when t2.ti is null then 0 else t2.ti end ti2,\
            case when t2.[PO(di+ic)] is null then 0 else t2.[PO(di+ic)] end 'PO2(di+ic)'\
            from\
            (\
	            select category, count(*) sku, sum(oh) 'oh', sum(iti) iti, sum(ti) ti, sum(PO) 'PO(di+ic)' from\
	            (\
		            select t2.category, t1.itemcode, oh*AvgCost oh, iti*avgcost iti, ti*AvgCost ti, (di+ic)*AvgCost PO from halifax t1, itemlist t2 \
		            where WareHouse like '%markham%' and t1.itemcode=t2.itemcode and t2.phaseout=1 \
		            and (oh!=0 or ti!=0 or iti!=0 or di!=0 or ic!=0) and AvgCost!=0\
	            ) t1\
	            group by category\
            ) t1\
            full outer join\
            (\
	            select category, count(*) sku, sum(oh) 'oh', sum(iti) iti, sum(ti) ti, sum(PO) 'PO(di+ic)'  from\
	            (\
		            select t2.category, t1.itemcode, oh*AvgCost oh, iti*avgcost iti,ti*AvgCost ti, (di+ic)*AvgCost PO from halifax t1, itemlist t2 \
		            where WareHouse like '%North%' and t1.itemcode=t2.itemcode and t2.phaseout=1 \
		            and (oh!=0 or ti!=0 or iti!=0 or di!=0 or ic!=0) and AvgCost!=0\
	            ) t1\
	            group by category\
            ) t2 on t1.category=t2.category)t1\
            order by t1.category"
    cursor.execute(sql)
    conn.commit()
    
    sql = "DECLARE @WH1 VARCHAR(10)\
            DECLARE @WH2 VARCHAR(10)\
            SET @WH1 = '124%'\
            SET @WH2 = '148%'\
            Insert into halifaxP2 select * from\
            (\
            select category,itemcode,sum(oh1) oh1, sum(sa1) sa1, sum(ti1) ti1, sum(iti1) iti1, sum(di1) di1, sum(ic1) ic1,\
            sum(oh2) oh2, sum(sa2) sa2, sum(ti2) ti2, sum(iti2) iti2, sum(di2) di2, sum(ic2) ic2, avgcost from\
            (select t2.category, t1.itemcode, \
            case when warehouse like @WH1 then t1.oh else 0 end oh1,\
            case when warehouse like @WH1 then t1.sa else 0 end sa1,\
            case when warehouse like @WH1 then t1.ti else 0 end ti1,\
            case when warehouse like @WH1 then t1.iti else 0 end iti1,\
            case when warehouse like @WH1 then t1.di else 0 end di1,\
            case when warehouse like @WH1 then t1.ic else 0 end ic1,\
            case when warehouse like @WH2 then t1.oh else 0 end oh2,\
            case when warehouse like @WH2 then t1.sa else 0 end sa2,\
            case when warehouse like @WH2 then t1.ti else 0 end ti2,\
            case when warehouse like @WH2 then t1.iti else 0 end iti2,\
            case when warehouse like @WH2 then t1.di else 0 end di2,\
            case when warehouse like @WH2 then t1.ic else 0 end ic2, \
            t1.avgcost \
            from halifax t1, itemlist t2\
            where t1.itemcode = t2.itemcode and t2.phaseout=1 \
            and (oh!=0 or sa!=0 or ti!=0 or iti!=0 or di!=0 or ic!=0))t1\
            group by category,itemcode,avgcost)t2\
            order by category, itemcode\
            "
    cursor.execute(sql)
    conn.commit()

    print('Write Halifax')
### 2. ItemWHSale
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ItemWareHouseSale.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("ItemWHSale")
    itemsale = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ItemWareHouseSale.xls")
    sheet_num = len(itemsale.sheet_names)
    # xls.parse(0)
    sql = 'INSERT INTO ItemWHSale ([ItemCode], [WareHouseName], [DateLastPO], [N], [S], [Sp], [St], [OH], \
    [OHmin], [Nn], [SA], [DI], [IC], [TI], [ITI], [AvgLDCost], [Date]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

    for numofsheet in range(0, sheet_num):
        for index, row in itemsale.parse(numofsheet).iterrows():
            Nn = row['Nn']
            if pd.isnull(Nn): Nn = 0 
            s = row['S']
            if pd.isnull(s): s = 0 
            sp = row['Sp']
            if pd.isnull(sp): sp = 0 
            st = row['St']
            if pd.isnull(st): st = 0 

            cursor.execute(sql, row['ItemCode'], row['WareHouseName'], row['DateLastPO'], row['N'],
                           s, sp, st, row['OH'], row['OHMin'], Nn, row['SA'], row['DI'],
                           row['IC'], row['TI'], row['ITI'], row['AvgLDCost'], today)
            conn.commit()
    print('Write ItemWHSale')

#### 3. ItemStock ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ItemStockList.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("ItemStock")
    itemstock = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ItemStockList.xls")
    sheet_num = len(itemstock.sheet_names)
    print('Read ItemStockList')

    for numofsheet in range(0, sheet_num):
        for index, row in itemstock.parse(numofsheet).iterrows():
            if row['Type'] != "Countable":
                continue
            apcost = row['AP Cost']
            marketcost = row['Market Cost']
            if math.isnan(apcost) and not (math.isnan(marketcost)):
                apcost = marketcost

            if not (math.isnan(apcost)):
                sql = "update itemlist set avgldcost=" + str(apcost) + ", price5=" + str(
                    row['Price5']) + " from itemlist t1 where t1.itemcode='" + row['Item'] + "';"
                for index1, row1 in wh.iterrows():
                    tmpwh = str(row1['WH ccid']) + " " + row1['WareHouseName']
                    sa = row[tmpwh]
                    if math.isnan(sa) or sa == 0: continue
                    sql += "INSERT INTO ItemStock ([Date], ItemCode, [WareHouseName], [SA]) VALUES ('" + today + "','" + \
                           row['Item'] + "','" + tmpwh + "'," + str(sa) + ");"
                cursor.execute(sql)
                conn.commit()
    print('Write ItemStockList')

#### 4. ItemSale ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ItemSalesReport.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("ItemSale")
    itemsale = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ItemSalesReport.xls")
    sheet_num = len(itemsale.sheet_names)
    print('Read ItemSale')

    sql = 'INSERT INTO ItemSale ([Order ccid], [Order Loc], [Ship ccid], [Ship Loc], [SalesOrderNo], [ShippingNo], [ShippingDate], [InvoiceNo], \
    [InvoiceDate], [Category], [ItemCode], [Type], [BQty], [UQty], [Quantity], [Price], [Discount], [Ext Price], [Ext Cost], [AvgCost], [AvgLDCost], \
    [AveTargetCost], [MarketCost], [S#T#Rebate]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

    for numofsheet in range(0, sheet_num):
        for index, row in itemsale.parse(numofsheet).iterrows():
            if pd.isnull(row['ShippingNo']):
                continue
            orderccid = row['Order ccid']
            if pd.isnull(orderccid):
                orderccid = ''
            shipccid = row['Ship ccid']
            if pd.isnull(shipccid):
                shipccid = ''
            cursor.execute(sql, orderccid, row['Order Loc'], shipccid, row['Ship Loc'], row['SalesOrderNo'],
                           row['ShippingNo'],
                           row['ShippingDate'], row['InvoiceNo'], row['InvoiceDate'], row['Category'], row['ItemCode'],
                           row['Type'], row['BQty'], row['UQty'],
                           row['Quantity'], row['Price'], row['Discount'], row['Ext Price'], row['Ext Cost'],
                           row['AvgCost'], row['AvgLDCost'], row['AveTargetCost'],
                           row['MarketCost'], row['S.T.Rebate'])
            conn.commit()
    print('Write ItemSale')

#### 5. AutoPO ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ExportReadyForRegPOP.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("AutoPO")
    autopo = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ExportReadyForRegPOP.xls")
    sheet_num = len(autopo.sheet_names)
    print('Read AutoPO')

    for numofsheet in range(0, sheet_num):
        for index, row in autopo.parse(numofsheet).iterrows():
            if row['POPType'] == "No POP Created" or row['POPType'] == "Buffer" or pd.isnull(row['SupplierName']): continue
            sql = 'INSERT INTO AutoPO ([AutoPOName], [BrandName], [ItemCode], [OrgCurrency], [Cost], [Current], [ConvertCAD], [WareHouseName], \
            [SupplierName], [RecevingHub], [CaseQty], [SugQty], [OrdQty], [POPType], [POPNo]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            cursor.execute(sql, row['AutoPOName'], row['BrandName'], row['ItemCode'], row['OrgCurrency'], row['Cost'],
                           row['Current'], row['ConvertCAD'], row['WareHouseName'], row['SupplierName'],
                           row['RecevingHub'], row['CaseQty'], row['SugQty'], row['OrdQty'],
                           row['POPType'], row['POPNo'])
            conn.commit()
    print('Write AutoPO')

    sql = "update autopo set date=SUBSTRING(autoponame,7,4)+'-'+SUBSTRING(autoponame,11,2)+'-'+SUBSTRING(autoponame,13,2) where date is null;"
    cursor.execute(sql)
    conn.commit()
    print('Write Date/Category in AutoPO')

#### 6. PO ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ExportPOItemList.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("PO")
    sql = "delete po where podate>'2019-06-30';"
    cursor.execute(sql)
    conn.commit()
    print('Delete PO Since 2019-07-01')

    po = pd.read_excel("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\ExportPOItemList.xls")
    print('Read PO')
    sql = 'INSERT INTO PO ([ItemCode], [Category], [WareHouseName], [SupplierCode], [PONo], [POPNo], [Status], [PODate], [Type], \
    [OrderQty], [VoidQty], [SpareQty], [Cost], [Cur], [Currency], [BO], [ETADate], [ReceiveNo], [RecTime]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    for index, row in po.iterrows():
        if row['Cur']!='CAD' and row['Cur']!='USD' : continue

        rt = row['RecTime']
        if pd.isnull(rt):
            rectime = ''
        else:
            rectime = str(rt)[:10]

        eta = row['ETADate']
        if pd.isnull(eta):
            eta = ''
        else:
            eta = str(eta)[:10]

        receiveno = row['ReceiveNo']
        if pd.isnull(receiveno):
            receiveno = ''

        popno = row['POPNo']
        if pd.isnull(popno):
            popno = ''

        topcategory = row['TopCategory']
        if pd.isnull(topcategory):
            topcategory = ''

        spareqty = row['SpareQty']
        if pd.isnull(spareqty):
            spareqty = 0

        cursor.execute(sql, row['ItemCode'], topcategory, row['WareHouseName'], row['SupplierCode'], row['PONo'],
                       popno, row['Status'], row['PODate'], row['Type'], row['OrderQty'], row['VoidQty'], spareqty, row['Cost'], row['Cur'],
                       row['Currency Rate'], row['BO'], eta, receiveno, rectime)
        conn.commit()

    sql = "delete po where status = 'Not Submitted' or status = 'not approved' or status = 'deleted'"
    cursor.execute(sql)
    conn.commit()

    print('Write PO')

#### 7. OTBSet ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\otbset.xlsx"):
    print(str(datetime.datetime.now())[11:19])
    print("OTBSet")
    otbset = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\otbset.xlsx")
    sheet_num = len(otbset.sheet_names)
    print('Read BudgetLogList')

    cursor.execute("delete otbset")
    conn.commit()
    print('Delete OTBSet')

    for numofsheet in range(0, sheet_num):
        for index, row in otbset.parse(numofsheet).iterrows():
            if pd.isnull(row['Brand ']): brand = ""
            else: brand = row['Brand ']
            if pd.isnull(row['Ceiling ']): ceiling = 0.0
            else: ceiling = row['Ceiling ']
            if pd.isnull(row['CeilingExpire ']): ceilingexpire = ""
            else: ceilingexpire = row['CeilingExpire ']
            
            if row['Enable '] == 0: continue

            sql = 'INSERT INTO otbset ([Category], [Brand], [Ratio], [Ceiling], [CeilingExpire], [Target], [Budget], [SalesCost], \
            [OnOrder], [TotalInventory], [BackOrder Cost ]) VALUES (?,?,?,?,?,?,?,?,?,?,?)'
            cursor.execute(sql, row['Category '], brand, row['Ratio '], ceiling, ceilingexpire,
                           row['Target '], row['Budget '], row['14Days SalesCost '], row['OnOrder '], row['Total Inventory '], row['BackOrder Cost '])
            conn.commit()
    print('Write OTBSet')
    
    #### 8. OTB ####
if path.exists("C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\BudgetLogList.xls"):
    print(str(datetime.datetime.now())[11:19])
    print("OTB")
    otb = pd.ExcelFile(
        "C:\Document\Solutions\Aging and Out of Stock in Stores\Analysis\\backup\BudgetLogList.xls")
    sheet_num = len(otb.sheet_names)
    print('Read BudgetLogList')

    for numofsheet in range(0, sheet_num):
        for index, row in otb.parse(numofsheet).iterrows():
            if pd.isnull(row['BrandName']): brandname = ""
            else: brandname = row['BrandName']
            if pd.isnull(row['SalesCost']): salecost = 0.0
            else: salecost = row['SalesCost']
            if pd.isnull(row['RefundCost']): refundcost = 0.0
            else: refundcost = row['RefundCost']
            if pd.isnull(row['BudgetCost']): budgetcost = 0.0
            else: budgetcost = row['BudgetCost']
            if pd.isnull(row['POCost']): pocost = 0.0
            else: pocost = row['POCost']
            if pd.isnull(row['Ceiling']): ceiling = 0.0
            else: ceiling = row['Ceiling']
            if pd.isnull(row['CeilingExpire']): ceilingexpire = ""
            else: ceilingexpire = row['CeilingExpire']
            if pd.isnull(row['TotalInventory']): totalinventory = 0.0
            else: totalinventory = row['TotalInventory']
            if pd.isnull(row['OnOrder']): onorder = ""
            else: onorder = row['OnOrder']
            if row['IsEnable'] == 0: continue

            sql = 'INSERT INTO otb ([LogDate], [Category], [Brand], [Ratio], [Ceiling], [CeilingExpire], [Target], [BudgetCost], \
            [SalesCost], [RefundCost], [POCost], [Remark], [TotalInventory], [OnOrder]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            cursor.execute(sql, row['LogDate'], row['CategoryName'], brandname, row['Ratio'], ceiling, ceilingexpire, row['Target'], budgetcost, 
                           salecost, refundcost, pocost, row['Remark'], totalinventory,onorder )
            conn.commit()
    print('Write OTB')

    sql = "update otb set date=SUBSTRING(LogDate,1,4)+'-'+SUBSTRING(LogDate,6,2)+'-'+SUBSTRING(LogDate,9,2) where date is null;"
    cursor.execute(sql)
    conn.commit()
    print('Write Date in OTB')

print(str(datetime.datetime.now())[11:19])
cursor.close()
conn.close()

#select * from halifaxnp1 order by category
#select * from halifaxnp2 order by category,itemcode
#select * from halifaxp1 order by category
#select * from halifaxp2 order by category,itemcode