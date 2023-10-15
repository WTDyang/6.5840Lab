@echo off

set test_count=100
set failed_tests=0
set total_tests=0

echo. > failed_log.txt

for /l %%i in (1, 1, %test_count%) do (

    echo Running test iteration %%i ...

    go test -run TestSnapshotInstallUnCrash2D > log.tmp

    rename log.tmp log%%i.log

    copy log%%i.log logs

    for /f "tokens=1 delims= " %%j in ('findstr /c:"--- FAIL:" log%%i.log') do (
        set /a failed_tests+=1
        copy log%%i.log error%%i.log
    )

    for /f "tokens=1 delims= " %%j in ('findstr /c:"PASS" log%%i.log') do (
        set /a total_tests+=1
    )

    findstr /c:"--- FAIL:" log%%i.log >> failed_log.txt
)

echo Total tests: %total_tests%
echo Failed tests: %failed_tests%
echo Failure rate: %failed_tests% / %total_tests% * 100 = %failed_tests:~0,-2%.%failed_tests:~-2%%%

xcopy /s log*.log logs