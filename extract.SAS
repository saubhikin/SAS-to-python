OPTIONS SYMBOLGEN;
    3    *--------------------------------------------------------------------
    3    | EXTRACT EMPLOYEE RECORDS FROM DB2 TABLES                         |
    3    --------------------------------------------------------------------;
    3    PROC SQL;
    6       CONNECT TO DB2 (SSID=DSNP);
    6       CREATE TABLE A AS
    8         SELECT * FROM CONNECTION TO DB2(
   11            SELECT EMP_ID             AS EMPID,
   18                   LAST_NAME              AS LASTNAME
   11            FROM   PCDS.EMPLOYEE
   11            WHERE  LAST_NAME   <>  '               ' AND
   18                   LAST_NAME   <> X'000000000000000000000000000000'
   11            );

    3    PROC SORT DATA=A;
    6       BY EMPID;
         *--------------------------------------------------------------------
    3    | WRITE EMPLOYEE RECORDS BASED ON LAST NAME                      |
    3    --------------------------------------------------------------------;

    3    DATA _NULL_;
    6       SET A;
    6       FILE OUT1;
    6       PUT  @001  EMPID       $CHAR07.
   11            @008  LASTNAME    $CHAR15.;

    3    RUN;