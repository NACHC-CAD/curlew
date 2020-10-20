@echo off

:: 
:: set the time
:: 

for /f "tokens=2 delims==" %%G in ('wmic os get localdatetime /value') do set datetime=%%G
set year=%datetime:~0,4%
set month=%datetime:~4,2%
set day=%datetime:~6,2%
set ts=%year%_%month%_%day%

echo.
echo * * * DOING BACK UPS * * *
echo %ts%
echo Backing up confluence...
mysqldump -u root -pflyWHEEL01! confluence > confluence_%ts%.sql
echo Backing up jira...
mysqldump -u root -pflyWHEEL01! jira > jira_%ts%.sql
echo * * * DONE * * *
echo.
echo.
