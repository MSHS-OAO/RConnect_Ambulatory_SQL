CREATE OR REPLACE PROCEDURE bin_hist(
    table_name IN VARCHAR2,
    column1    IN VARCHAR2, 
    bin_col    IN  VARCHAR2,
    bin_value  IN NUMBER,   
    max_value  IN NUMBER,  
    min_value  IN NUMBER)
IS
    sql_statement VARCHAR2(4000);
BEGIN
    sql_statement := 'alter table ' || table_name || ' add (' || bin_col || ' VARCHAR2(40))'; 
      EXECUTE IMMEDIATE sql_statement;
      
    sql_statement := 'update ' || table_name || ' set ' || bin_col || ' =  case ' ||
    'when floor(' || column1 || ' / :bin_value) * :bin_value > :max_value then ''>'' || :max_value ' ||
    'when floor(' || column1 || ' / :bin_value) * :bin_value < :min_value then ''<'' || :min_value ' ||
    'else to_char(floor(' || column1 || ' / :bin_value) * :bin_value) ' ||
    'end'; 
    
    dbms_output.put_line(sql_statement);
    
    EXECUTE IMMEDIATE sql_statement 
    USING bin_value, bin_value, max_value, max_value, bin_value, bin_value, min_value, min_value, bin_value, bin_value;
    
    COMMIT;
END;
