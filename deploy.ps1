if( $args.Length -gt 0 ) { $targets = $args}
else { $targets = @("s3", "lambda", "verify")}
$7z = 'C:\Program Files\7-Zip\7z.exe'

if ($targets.Contains("s3")) {
    aws s3 sync www s3://energy.pettijohn.com --delete
}

if ($targets.Contains("lambda")) {
    if(Test-Path lambda-deploy-package.zip) {
        Remove-Item lambda-deploy-package.zip
    }
    & $7z a -x!**\__pycache__ -x!__pycache__ lambda-deploy-package.zip .\awslambda\*
    aws lambda update-function-code --function-name "EnergyMonitorSave" --zip-file fileb://lambda-deploy-package.zip
}


if ($targets.Contains("verify")) {
    $i = 4
    while ($i -gt 0) {
        $bucketId = (get-date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm+0000")
        Write-Output "Checking for $bucketId"
        Write-Output "{""device_id"":{""S"":""arn:aws:iot:us-east-1:422446087002:thing/EMonPi""},""bucket_id"":{""S"":""${bucketId}""}}" | out-file key.json -Encoding ASCII
        aws dynamodb get-item --table-name "EnergyMonitor.Minute" --consistent-read --key file://key.json
        remove-item key.json
        Start-Sleep 20
        $i--
    }
}