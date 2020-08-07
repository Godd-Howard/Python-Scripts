import pyodbc
import xlrd

conn = pyodbc.connect('Driver={SQL Server}; Server=XX; Database=XX; Trusted_Connection=Yes;')
cursor = conn.cursor() 

wkBook = xlrd.open_workbook("X/TMW Trailer Cleanup.xlsx")
sheet = wkBook.sheet_by_index(0)

trlType4 = None

i = 1
for i in range(sheet.nrows):
	trlID = sheet.cell_value(i, 0)
	if type(trlID) is not str:
		trlID = int(trlID)
		strlID = str(trlID)
	else:
		strlID = str(trlID)
	trlType1 = sheet.cell_value(i, 2)
	trlType2 = sheet.cell_value(i, 4)
	trlType3 = sheet.cell_value(i, 6)

	cursor.execute("UPDATE trailerprofile SET trl_type1 = ?, trl_type2 = ?, trl_type3 = ?, trl_type4 = ? WHERE trl_id = ?", trlType1, trlType2, trlType3, trlType4, strlID)
	conn.commit()

conn.close()