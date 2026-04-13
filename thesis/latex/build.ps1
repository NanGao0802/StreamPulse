# ============================================================
#  一键编译脚本：xelatex -> bibtex -> xelatex -> xelatex
#  用法：在 PowerShell 中执行  .\build.ps1
# ============================================================

$miktex = "C:\Users\win\AppData\Local\Programs\MiKTeX\miktex\bin\x64"
$env:PATH = "$miktex;$env:PATH"

Write-Host "[1/4] xelatex (第一遍)..." -ForegroundColor Cyan
xelatex -interaction=nonstopmode main.tex | Out-Null

Write-Host "[2/4] bibtex..." -ForegroundColor Cyan
bibtex main | Out-Null

Write-Host "[3/4] xelatex (第二遍，解析引用)..." -ForegroundColor Cyan
xelatex -interaction=nonstopmode main.tex | Out-Null

Write-Host "[4/4] xelatex (第三遍，确定书签/目录)..." -ForegroundColor Cyan
xelatex -interaction=nonstopmode main.tex | Out-Null

$pdf = "main.pdf"
if (Test-Path $pdf) {
    $pages = (Get-Item $pdf).Length
    Write-Host "✓ 编译成功：$pdf（$('{0:N0}' -f $pages) 字节）" -ForegroundColor Green
    Start-Process $pdf
} else {
    Write-Host "✗ 未找到 main.pdf，请检查 main.log" -ForegroundColor Red
}
