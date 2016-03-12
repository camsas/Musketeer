grammar RLPlus;

options {
  language = C;
  ASTLabelType=pANTLR3_BASE_TREE;
  output = AST;
  backtrack = true;
}

tokens {
  CREATE_RELATION = 'CREATE RELATION';
  BLACK_BOX = 'BLACK BOX';
  AGG = 'AGG';
  COUNT = 'COUNT';
  CROSS_JOIN = 'CROSS JOIN';
  DIFFERENCE = 'DIFFERENCE';
  DISTINCT = 'DISTINCT';
  DIV = 'DIV';
  INTERSECT = 'INTERSECT';
  JOIN = 'JOIN';
  GROUP_BY = 'GROUP BY';
  MAXIMUM = 'MAXIMUM';
  MINIMUM = 'MINIMUM';
  MUL = 'MUL';
  PROJECT = 'PROJECT';
  RENAME = 'RENAME';
  SELECT = 'SELECT';
  SORT_INC = 'SORT INC';
  SORT_DEC = 'SORT DEC';
  SUB = 'SUB';
  SUM = 'SUM';
  UNION = 'UNION';
  WHILE = 'WHILE';
  AND = 'AND';
  NOT = 'NOT';
  EMPTY_NODE = 'EMPTY_NODE';
  OR = 'OR';
  PLUS = '+';
  MINUS = '-';
  DIVIDE = '/';
  MULTIPLY = '*';
  LESS = '<';
  LESS_EQUAL = '<=';
  EQUAL = '=';
  NOT_EQUAL = '<>';
  GREATER = '>';
  GREATER_EQUAL = '>=';
  INTEGER = 'INTEGER';
  DOUBLE = 'DOUBLE';
  STRING = 'STRING';
  BOOLEAN = 'BOOLEAN';
  OUTPUT = 'OUTPUT';
  UDF = 'UDF';
}

AS            : 'AS';
COLUMNS       : 'COLUMNS';
DO            : 'DO';
FROM          : 'FROM';
WHERE         : 'WHERE';
ENDCOLS       : 'ENDCOLS';
DELIMITER     : 'DELIMITER';
ON            : 'ON';
DOT           : '.';
LPAREN        : '(';
RPAREN        : ')';
LSQUARE       : '[';
RSQUARE       : ']';
COMMA         : ',';
WITH          : 'WITH';
GRAPH_CHI     : 'GraphChi';
HADOOP        : 'Hadoop';
METIS         : 'Metis';
POWER_GRAPH   : 'PowerGraph';
POWER_LYRA    : 'PowerLyra';
SPARK         : 'Spark';
WILD_CHERRY   : 'WildCherry';

start_rule : expr EOF;

expr
    : createExpr (COMMA! expr)*
    | aggExpr (COMMA! expr)*
    | countExpr (COMMA! expr)*
    | crossJoinExpr (COMMA! expr)*
    | differenceExpr (COMMA! expr)*
    | distinctExpr (COMMA! expr)*
    | divExpr (COMMA! expr)*
    | intersectionExpr (COMMA! expr)*
    | joinExpr (COMMA! expr)*
    | minimumExpr (COMMA! expr)*
    | maximumExpr (COMMA! expr)*
    | mulExpr (COMMA! expr)*
    | projectExpr (COMMA! expr)*
    | renameExpr (COMMA! expr)*
    | selectExpr (COMMA! expr)*
    | sortExpr (COMMA! expr)*
    | subExpr (COMMA! expr)*
    | sumExpr (COMMA! expr)*
    | unionExpr (COMMA! expr)*
    | whileExpr (COMMA! expr)*
    | blackBoxExpr (COMMA! expr)*
    | udfExpr (COMMA! expr)*
    | attributeExpr
    ;

createExpr
    : CREATE_RELATION ATTRIBUTE WITH COLUMNS LPAREN typeExpr RPAREN -> ^(CREATE_RELATION ATTRIBUTE typeExpr)
    ;

blackBoxExpr
    : BLACK_BOX framework PATH AS ATTRIBUTE WITH COLUMNS LPAREN typeExpr RPAREN-> ^(BLACK_BOX framework PATH ATTRIBUTE typeExpr)
    ;

udfExpr
    : UDF PATH ATTRIBUTE ON ATTRIBUTE AS ATTRIBUTE WITH COLUMNS LPAREN typeExpr RPAREN -> ^(UDF PATH ATTRIBUTE ATTRIBUTE ATTRIBUTE typeExpr)
    ;

aggExpr
    : AGG LSQUARE attributeExpr COMMA mathOperator RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(AGG attributeExpr ENDCOLS mathOperator condition expr DELIMITER attributeExpr ATTRIBUTE)
    | AGG LSQUARE attributeExpr COMMA mathOperator RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(AGG attributeExpr ENDCOLS mathOperator condition expr DELIMITER EMPTY_NODE ATTRIBUTE)
    | AGG LSQUARE attributeExpr COMMA mathOperator RSQUARE FROM LPAREN expr RPAREN GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(AGG attributeExpr ENDCOLS mathOperator EMPTY_NODE expr DELIMITER attributeExpr ATTRIBUTE)
    | AGG LSQUARE attributeExpr COMMA mathOperator RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(AGG attributeExpr ENDCOLS mathOperator EMPTY_NODE expr DELIMITER EMPTY_NODE ATTRIBUTE)
    ;

countExpr
    : COUNT LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(COUNT ATTRIBUTE condition expr DELIMITER attributeExpr ATTRIBUTE)
    | COUNT LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(COUNT ATTRIBUTE condition expr DELIMITER EMPTY_NODE ATTRIBUTE)
    | COUNT LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(COUNT ATTRIBUTE EMPTY_NODE expr DELIMITER attributeExpr ATTRIBUTE)
    | COUNT LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(COUNT ATTRIBUTE EMPTY_NODE expr DELIMITER EMPTY_NODE ATTRIBUTE)
    ;

crossJoinExpr
    : LPAREN expr RPAREN CROSS_JOIN LPAREN expr RPAREN AS ATTRIBUTE -> ^(CROSS_JOIN expr expr ATTRIBUTE)
    ;

differenceExpr
    : LPAREN expr RPAREN DIFFERENCE LPAREN expr RPAREN AS ATTRIBUTE -> ^(DIFFERENCE expr expr ATTRIBUTE)
    ;

distinctExpr
    : DISTINCT FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(DISTINCT expr ATTRIBUTE)
    ;

divExpr
    : DIV LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(DIV attrOrNum attrOrNum condition expr ATTRIBUTE)
    | DIV LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(DIV attrOrNum attrOrNum EMPTY_NODE expr ATTRIBUTE)
    ;

intersectionExpr
    : LPAREN expr RPAREN INTERSECT LPAREN expr RPAREN AS ATTRIBUTE -> ^(INTERSECT expr expr ATTRIBUTE)
    ;

joinExpr
    : LPAREN expr RPAREN JOIN LPAREN expr RPAREN ON attributeExpr AND attributeExpr AS ATTRIBUTE -> ^(JOIN expr expr attributeExpr ENDCOLS attributeExpr ENDCOLS ATTRIBUTE)
    ;

maximumExpr
    : MAXIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(MAXIMUM attributeExpr ENDCOLS condition expr DELIMITER attributeExpr ATTRIBUTE)
    | MAXIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(MAXIMUM attributeExpr ENDCOLS condition expr DELIMITER EMPTY_NODE ATTRIBUTE)
    | MAXIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(MAXIMUM attributeExpr ENDCOLS EMPTY_NODE expr DELIMITER attributeExpr ATTRIBUTE)
    | MAXIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(MAXIMUM attributeExpr ENDCOLS EMPTY_NODE expr DELIMITER EMPTY_NODE ATTRIBUTE)
    ;

minimumExpr
    : MINIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(MINIMUM attributeExpr ENDCOLS condition expr DELIMITER attributeExpr ATTRIBUTE)
    | MINIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(MINIMUM attributeExpr ENDCOLS condition expr DELIMITER EMPTY_NODE ATTRIBUTE)
    | MINIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN GROUP_BY LSQUARE attributeExpr RSQUARE AS ATTRIBUTE -> ^(MINIMUM attributeExpr ENDCOLS EMPTY_NODE expr DELIMITER attributeExpr ATTRIBUTE)
    | MINIMUM LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(MINIMUM attributeExpr ENDCOLS EMPTY_NODE expr DELIMITER EMPTY_NODE ATTRIBUTE)
    ;

mulExpr
    : MUL LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(MUL attrOrNum attrOrNum condition expr ATTRIBUTE)
    | MUL LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(MUL attrOrNum attrOrNum EMPTY_NODE expr ATTRIBUTE)
    ;

projectExpr
    : PROJECT LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(PROJECT attributeExpr ENDCOLS condition expr ATTRIBUTE)
    | PROJECT LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(PROJECT attributeExpr ENDCOLS EMPTY_NODE expr ATTRIBUTE)
    ;

renameExpr
    : RENAME LSQUARE attributeExpr RSQUARE LPAREN expr RPAREN -> ^(RENAME attributeExpr expr)
    ;

selectExpr
    : SELECT LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE-> ^(SELECT attributeExpr ENDCOLS condition expr ATTRIBUTE)
    | SELECT LSQUARE attributeExpr RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE-> ^(SELECT attributeExpr ENDCOLS EMPTY_NODE expr ATTRIBUTE)
    ;

sortExpr
    : SORT_INC ON LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(SORT_INC ATTRIBUTE condition expr ATTRIBUTE)
    | SORT_INC ON LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(SORT_INC ATTRIBUTE EMPTY_NODE expr ATTRIBUTE)
    | SORT_DEC ON LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(SORT_DEC ATTRIBUTE condition expr ATTRIBUTE)
    | SORT_DEC ON LSQUARE ATTRIBUTE RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(SORT_DEC ATTRIBUTE EMPTY_NODE expr ATTRIBUTE)
    ;

subExpr
    : SUB LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(SUB attrOrNum attrOrNum condition expr ATTRIBUTE)
    | SUB LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(SUB attrOrNum attrOrNum EMPTY_NODE expr ATTRIBUTE)
    ;

sumExpr
    : SUM LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN WHERE LSQUARE condition RSQUARE AS ATTRIBUTE -> ^(SUM attrOrNum attrOrNum condition expr ATTRIBUTE)
    | SUM LSQUARE attrOrNum COMMA attrOrNum RSQUARE FROM LPAREN expr RPAREN AS ATTRIBUTE -> ^(SUM attrOrNum attrOrNum EMPTY_NODE expr ATTRIBUTE)
    ;

unionExpr
    : LPAREN expr RPAREN UNION LPAREN expr RPAREN AS ATTRIBUTE -> ^(UNION expr expr ATTRIBUTE)
    ;

whileExpr
    : WHILE LSQUARE condition RSQUARE DO LPAREN expr RPAREN -> ^(WHILE condition expr)
    ;

condition
    : LPAREN! simpleCondition RPAREN!
    | LPAREN simpleCondition AND condition RPAREN -> ^(AND simpleCondition condition)
    | LPAREN simpleCondition OR condition RPAREN -> ^(OR simpleCondition condition)
    | NOT condition -> ^(NOT condition)
    ;

simpleCondition
    : operand comparison operand -> ^(comparison operand operand)
    ;

operand
    : STRING_VALUE
    | DOUBLE_VALUE
    | INT_VALUE
    | ATTRIBUTE
    ;

mathOperator
    : PLUS
    | MINUS
    | DIVIDE
    | MULTIPLY
    ;

comparison
    : LESS
    | LESS_EQUAL
    | EQUAL
    | NOT_EQUAL
    | GREATER
    | GREATER_EQUAL
    ;

attrOrNum
    : ATTRIBUTE
    | DOUBLE_VALUE
    | INT_VALUE
    ;

// Can be list of columns or relations.
attributeExpr
    : ATTRIBUTE (COMMA! ATTRIBUTE)*
    ;

typeExpr
    : (INTEGER | DOUBLE | STRING | BOOLEAN) (COMMA! (INTEGER | DOUBLE | STRING | BOOLEAN))*
    ;

framework
    : GRAPH_CHI | HADOOP | METIS | POWER_GRAPH | POWER_LYRA | SPARK
    ;

//============================= Lexer rules ====================================

fragment LETTER : 'a'..'z' | 'A'..'Z';

fragment DIGIT : '0'..'9';

fragment EXPONENT : ('e' | 'E') ( PLUS | MINUS )? (DIGIT)+;

PATH : ('/' LETTER (LETTER | DIGIT | '_' | '.' )*)+;

// Can represent column or relation.
ATTRIBUTE : LETTER (LETTER | DIGIT | '_')*;

STRING_VALUE : ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"' )+;

INT_VALUE : (DIGIT)+;
DOUBLE_VALUE : (DIGIT)+ ( DOT (DIGIT)+ (EXPONENT)? | EXPONENT)?;

WHITESPACE : (' ' | '\r' | '\t' | '\n') {$channel=HIDDEN;};
