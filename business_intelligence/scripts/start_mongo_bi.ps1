# --- start_mongo_bi.ps1 ---
# This script starts the MongoDB BI Connector (mongosqld)
# to make your MongoDB database accessible to Power BI via ODBC.

$mongoUri = "mongodb://localhost:27100"
$connectorPath = "C:\Program Files\MongoDB\Connector for BI\2.14\bin\mongosqld.exe"

Write-Host "--------------------------------------------------"
Write-Host "Starting the MongoDB BI Connector"
Write-Host "ℹ️ MongoDB Target: $mongoUri"  
Write-Host "--------------------------------------------------"

Start-Process -FilePath $connectorPath -ArgumentList "--mongo-uri", $mongoUri -NoNewWindow
Write-Host "✅ The BI Connector is running on port 3307. You can now launch Power BI."