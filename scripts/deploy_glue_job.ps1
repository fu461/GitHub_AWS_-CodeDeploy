param(
    [string]$JobName = "DataProcessingJob",
    [string]$ScriptPath = ".\glue\etl_job.py",
    [string]$RoleArn = "arn:aws:iam::123456789012:role/GlueExecutionRole",
    [string]$Region = "us-east-1"
)

Write-Host "=== 部署 Glue Job ===" -ForegroundColor Cyan
Write-Host "Job 名称: $JobName"
Write-Host "脚本路径: $ScriptPath"
Write-Host "IAM 角色: $RoleArn"
Write-Host "区域: $Region"
Write-Host ""

# 检查文件是否存在
if (-not (Test-Path $ScriptPath)) {
    Write-Host "错误: 脚本文件不存在: $ScriptPath" -ForegroundColor Red
    exit 1
}

# 读取脚本内容
$scriptContent = Get-Content $ScriptPath -Raw
Write-Host "脚本大小: $($scriptContent.Length) 字符" -ForegroundColor Yellow

# 配置 AWS
Set-DefaultAWSRegion -Region $Region

try {
    # 检查 Job 是否已存在
    Write-Host "检查现有 Job..." -ForegroundColor Yellow
    $existingJob = Get-GLUEJob -JobName $JobName -ErrorAction SilentlyContinue
    
    # 准备 Job 参数
    $jobParams = @{
        Name = $JobName
        Role = $RoleArn
        Command = @{
            Name = "glueetl"
            ScriptLocation = "s3://aws-glue-assets-$((Get-STSCallerIdentity).Account)-$Region/scripts/$JobName.py"
            PythonVersion = "3"
        }
        DefaultArguments = @{
            "--job-bookmark-option" = "job-bookmark-enable"
            "--enable-continuous-cloudwatch-log" = "true"
            "--enable-metrics" = "true"
            "--TempDir" = "s3://aws-glue-assets-$((Get-STSCallerIdentity).Account)-$Region/temporary/"
        }
        MaxRetries = 2
        Timeout = 60
        WorkerType = "G.1X"
        NumberOfWorkers = 2
        GlueVersion = "4.0"
    }
    
    if ($existingJob) {
        Write-Host "更新现有 Job: $JobName" -ForegroundColor Yellow
        Update-GLUEJob -JobName $JobName -JobUpdate $jobParams
        $action = "更新"
    } else {
        Write-Host "创建新 Job: $JobName" -ForegroundColor Green
        New-GLUEJob @jobParams
        $action = "创建"
    }
    
    # 上传脚本到 S3
    Write-Host "上传脚本到 S3..." -ForegroundColor Yellow
    $tempFile = [System.IO.Path]::GetTempFileName() + ".py"
    $scriptContent | Out-File -FilePath $tempFile -Encoding UTF8
    
    Write-S3Object -BucketName "aws-glue-assets-$((Get-STSCallerIdentity).Account)-$Region" `
                   -Key "scripts/$JobName.py" `
                   -File $tempFile
    
    Remove-Item $tempFile
    
    Write-Host "`n=== 部署完成 ===" -ForegroundColor Green
    Write-Host "操作: $action"
    Write-Host "Job: $JobName"
    Write-Host "状态: 成功" -ForegroundColor Green
    
    # 测试 Job 运行
    Write-Host "`n是否要测试运行 Job? (y/n)" -ForegroundColor Cyan
    $response = Read-Host
    if ($response -eq 'y') {
        Write-Host "启动 Job 运行..." -ForegroundColor Yellow
        $run = Start-GLUEJobRun -JobName $JobName
        Write-Host "Job Run ID: $($run.Id)" -ForegroundColor Green
    }
    
} catch {
    Write-Host "`n=== 部署失败 ===" -ForegroundColor Red
    Write-Host "错误: $_" -ForegroundColor Red
    exit 1
}