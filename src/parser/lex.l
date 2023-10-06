    /* keywords are case insensitive */
/*大小写不敏感 */
%option caseless
    /* we don't need yywrap() function */
/*%option noyywrap 指令可以禁用自动生成的 yywrap() 函数。这意味着词法分析器将不会尝试自动处理文件的包装问题，而是需要开发人员手动处理多个输入文件的切换。*/
%option noyywrap
    /* we don't need yyunput() function */
/*禁用默认生成的 unput() 函数。这意味着无法将符号重新放回输入流。*/
%option nounput
    /* we don't need input() function */
/*禁用默认生成的 yyin 输入流。这表示需要手动设置输入流，而不使用默认的标准输入流。*/
%option noinput
    /* enable location */
/*与 Bison 解析器生成器一起使用*/
%option bison-bridge
%option bison-locations

%{
#include "ast.h"
#include "yacc.tab.h"
#include "errors.h"
#include <iostream>

// automatically update location
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for (int i = 0; yytext[i] != '\0'; i++) { \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 1; \
        } else { \
            yylloc->last_column++; \
        } \
    }

%}


/* alpha：匹配一个字母（大小写不敏感）。 */
/* digit：匹配一个数字。 */
/* white_space：匹配一个或多个空格或制表符。 */
/* new_line：匹配一个换行符。可以是 \r、\n 或者组合 \r\n。 */
/* sign：匹配加号或减号。 */
/* identifier：匹配标识符，由字母开头，后跟零个或多个字母、数字或下划线。 */
/* value_int：匹配整数值。可以包含可选的正负号，后跟一个或多个数字。 */
/* value_float：匹配浮点数值。可以包含可选的正负号，后跟一个或多个数字，然后是小数点和一个或多个数字。 */
/* value_string：匹配字符串值。由单引号括起来，可以包含任意字符（除了单引号本身的字符）。 */
/* single_op：匹配一些特定的单字符操作符，例如分号、括号、逗号、乘号、等号、大于号、小于号和句点。 */
/* value_path: 文件路径。*/

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.({digit}+)?
value_string '[^']*'
value_path [\.|\/][^ \t]+\.csv
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."|{sign}" "

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { /* ignore new line */ }
    /* keywords */
"SHOW" { return SHOW; }
"BEGIN" { return TXN_BEGIN; }
"COMMIT" { return TXN_COMMIT; }
"ABORT" { return TXN_ABORT; }
"ROLLBACK" { return TXN_ROLLBACK; }
"TABLES" { return TABLES; }
"CREATE" { return CREATE; }
"TABLE" { return TABLE; }
"DROP" { return DROP; }
"DESC" { return DESC; }
"INSERT" { return INSERT; }
"INTO" { return INTO; }
"VALUES" { return VALUES; }
"DELETE" { return DELETE; }
"FROM" { return FROM; }
"WHERE" { return WHERE; }
"UPDATE" { return UPDATE; }
"SET" { return SET; }
"SELECT" { return SELECT; }
"COUNT" { return COUNT; }
"MAX" { return MAX; }
"MIN" { return MIN; }
"SUM" { return SUM; }
"AS" { return AS; }
"INT" { return INT; }
"CHAR" { return CHAR; }
"FLOAT" { return FLOAT; }
"BIGINT" { return BIGINT; }
"DATETIME" {return DATETIME;}
"INDEX" { return INDEX; }
"AND" { return AND; }
"JOIN" {return JOIN;}
"EXIT" { return EXIT; }
"HELP" { return HELP; }
"ORDER" { return ORDER; }
"BY" {  return BY;  }
"ASC" { return ASC; }
"LIMIT" {return LIMIT;}
"LOAD" {return LOAD;}
"OUTPUT_FILE" {return OUTPUT_FILE;}
"ON" {return ON;}
"OFF" {return OFF;}
    /* operators */
">=" { return GEQ; }
"<=" { return LEQ; }
"<>" { return NEQ; }
{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->sv_str = yytext;
    return IDENTIFIER;
}
    /* literals */
{value_int} {
    const std::string LONG_LONG_MAX_STR = std::to_string(0x7fffffffffffffffLL);
    const std::string LONG_LONG_MIN_STR = std::to_string((-0x7fffffffffffffffLL - 1LL));
    std::string yytext_str = std::string(yytext, strlen(yytext));

    auto yyis_invalid = [&] (const std::string& s) {
        if(s[0]=='-') {
            if(s.size() == LONG_LONG_MIN_STR.size()){
                return s>LONG_LONG_MIN_STR;
            }
            else{
                return s.size() > LONG_LONG_MIN_STR.size();
            }
        }
        else{
            if(s.size() == LONG_LONG_MAX_STR.size()){
                return s>LONG_LONG_MAX_STR;
            }
            else{
                return s.size() > LONG_LONG_MAX_STR.size();
            }
        }
    };

    if(yyis_invalid(yytext_str)) {
        throw InternalError("Number OverFlow!");
    }
    else {
        if (atoll(yytext) > 2147483647 || atoll(yytext) < -2147483648) {
            yylval->sv_bigint = atoll(yytext);
            return VALUE_BIGINT;
        }
        else {
            yylval->sv_int = atoi(yytext);
            return VALUE_INT;
        }
    }
}
{value_float} {
    yylval->sv_float = atof(yytext);
    return VALUE_FLOAT;
}
{value_string} {
    yylval->sv_str = std::string(yytext + 1, strlen(yytext) - 2);
    return VALUE_STRING;
}
{value_path} {
    yylval->sv_str = yytext;
    return VALUE_PATH;
}
    /* EOF */
<<EOF>> { return T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%
