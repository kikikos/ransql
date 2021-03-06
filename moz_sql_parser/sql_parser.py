# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import ast
import sys

from pyparsing import Word, delimitedList, Optional, Combine, Group, alphas, alphanums, Forward, restOfLine, Keyword, Literal, ParserElement, infixNotation, opAssoc, Regex, MatchFirst, ZeroOrMore

ParserElement.enablePackrat()

# THE PARSING DEPTH IS NASTY
sys.setrecursionlimit(2000)


DEBUG = False
END = None

all_exceptions = {}


def record_exception(instring, loc, expr, exc):
    # if DEBUG:
    #     print ("Exception raised:" + _ustr(exc))
    es = all_exceptions.setdefault(loc, [])
    es.append(exc)


def nothing(*args):
    pass


if DEBUG:
    debug = (None, None, None)
else:
    debug = (nothing, nothing, record_exception)

join_keywords = {
    "join",
    "full join",
    "cross join",
    "inner join",
    "left join",
    "right join",
    "full outer join",
    "right outer join",
    "left outer join",
}
keywords = {
    "add",  # ransql: add (col1, col2) as 3rd col
    "and",
    "as",
    "asc",
    "avg",  # ransql: avg operator
    "between",
    "case",
    "collate nocase",
    "desc",
    "divide",  # ransql: divide operator
    "else",
    "end",
    "from",
    "group by",
    "having",
    "in",
    "not in",
    "is",
    "limit",
    "obj", #objectize a list
    "offset",
    "like",
    "minus",  # ransql: minus operator
    "multiply",  # ransql: multiply/times operator
    "not between",
    "not like",
    "on",
    "or",
    "order by",
    "select",
    "then",
    "time",  # ransql: fetch time interval
    "to",  # ransql: to a 3rd app
    "top",  # ransql: top (1,10) top 1-10
    "union",
    "union all",
    "using",
    "when",
    "where",
    "with"
} | join_keywords


locs = locals()

reserved = []

for k in keywords:
    name = k.upper().replace(" ", "")
    locs[name] = value = Keyword(k, caseless=True).setName(
        k.lower()).setDebugActions(*debug)
    reserved.append(value)

RESERVED = MatchFirst(reserved)

KNOWN_OPS = [
    (BETWEEN, AND),
    (NOTBETWEEN, AND),
    Literal("||").setName("concat").setDebugActions(*debug),
    Literal("*").setName("mul").setDebugActions(*debug),
    Literal("/").setName("div").setDebugActions(*debug),
    Literal("+").setName("add").setDebugActions(*debug),
    Literal("-").setName("sub").setDebugActions(*debug),
    Literal("<>").setName("neq").setDebugActions(*debug),
    Literal(">").setName("gt").setDebugActions(*debug),
    Literal("<").setName("lt").setDebugActions(*debug),
    Literal(">=").setName("gte").setDebugActions(*debug),
    Literal("<=").setName("lte").setDebugActions(*debug),
    Literal("=").setName("eq").setDebugActions(*debug),
    Literal("==").setName("eq").setDebugActions(*debug),
    Literal("!=").setName("neq").setDebugActions(*debug),
    IN.setName("in").setDebugActions(*debug),
    NOTIN.setName("nin").setDebugActions(*debug),
    IS.setName("is").setDebugActions(*debug),
    LIKE.setName("like").setDebugActions(*debug),
    NOTLIKE.setName("nlike").setDebugActions(*debug),
    OR.setName("or").setDebugActions(*debug),
    AND.setName("and").setDebugActions(*debug)
]


def to_json_operator(instring, tokensStart, retTokens):
    # ARRANGE INTO {op: params} FORMAT
    tok = retTokens[0]
    for o in KNOWN_OPS:
        if isinstance(o, tuple):
            if o[0].match == tok[1]:
                op = o[0].name
                break
        elif o.match == tok[1]:
            op = o.name
            break
    else:
        if tok[1] == COLLATENOCASE.match:
            op = COLLATENOCASE.name
            return {op: tok[0]}
        else:
            raise "not found"

    if op == "eq":
        if tok[2] == "null":
            return {"missing": tok[0]}
        elif tok[0] == "null":
            return {"missing": tok[2]}
    elif op == "neq":
        if tok[2] == "null":
            return {"exists": tok[0]}
        elif tok[0] == "null":
            return {"exists": tok[2]}
    elif op == "is":
        if tok[2] == 'null':
            return {"missing": tok[0]}
        else:
            return {"exists": tok[0]}

    return {op: [tok[i * 2] for i in range(int((len(tok) + 1) / 2))]}


def to_json_call(instring, tokensStart, retTokens):
    # ARRANGE INTO {op: params} FORMAT
    tok = retTokens
    op = tok.op.lower()

    if op == "-":
        op = "neg"

    params = tok.params
    if not params:
        params = None
    elif len(params) == 1:
        params = params[0]
    return {op: params}


def to_case_call(instring, tokensStart, retTokens):
    tok = retTokens
    cases = list(tok.case)
    elze = getattr(tok, "else", None)
    if elze:
        cases.append(elze)
    return {"case": cases}


def to_when_call(instring, tokensStart, retTokens):
    tok = retTokens
    return {"when": tok.when, "then": tok.then}


def to_join_call(instring, tokensStart, retTokens):
    tok = retTokens

    if tok.join.name:
        output = {tok.op: {"name": tok.join.name, "value": tok.join.value}}
    else:
        output = {tok.op: tok.join}

    if tok.on:
        output['on'] = tok.on

    if tok.using:
        output['using'] = tok.using
    return output


def to_select_call(instring, tokensStart, retTokens):
    tok = retTokens[0].asDict()
    #print("select tok:", tok)

    if tok.get('value')[0][0] == '*':
        return '*'
    else:
        return tok

# todo


def to_time_call(instring, tokensStart, retTokens):
    #print("tim call retTokens:",retTokens )
    """tok = retTokens[0].asDict()
    time_interval = tok.time
    
    
    print("tim call retTokens[0]:",retTokens[0] )
    print("tim call retTokens[1]:",retTokens[1])
    print("tim call retTokens[1][0]:",retTokens[1][0])
    
    key=""
    for k in retTokens[1][0]: 
        print("k:{} and v".format(k))
        key = k
        break #only read first one


    value =retTokens[1][0][key][0]
    item = {key:value}

    retTokens[1] = {'time': [item]}
    """
    """
    print("tim call retTokens[1]*:",retTokens[1])
    print("tim call retTokens*:",retTokens)
    """
    return {"time":retTokens}


def to_to_call(instring, tokensStart, retTokens):
    
    #print("to_to_call retTokens:",retTokens )
    
    #print("to_to_call retTokens[0]:",retTokens[0] )
    
    #print("to_to_call retTokens[1]:",retTokens[1])
    
    #retTokens[2] = {"to":retTokens[2]}
    #print("to_to_call retTokens*", retTokens)
    return {"to":retTokens}


def to_union_call(instring, tokensStart, retTokens):
    """
    print("instring:",instring)
    print("tokensStart:",tokensStart)

    print('to_union_call:', retTokens)
    """

    tok = retTokens[0].asDict()
    unions = tok['from']['union']
    if len(unions) == 1:
        output = unions[0]
    else:
        if not tok.get('orderby') and not tok.get('limit'):
            return tok['from']
        else:
            output = {"from": {"union": unions}}

    if tok.get('orderby'):
        output["orderby"] = tok.get('orderby')
    if tok.get('limit'):
        output["limit"] = tok.get('limit')
    if tok.get('time'):
        output["time"] = tok.get('time')
    return output


def unquote(instring, tokensStart, retTokens):
    val = retTokens[0]
    if val.startswith("'") and val.endswith("'"):
        val = "'"+val[1:-1].replace("''", "\\'")+"'"
        # val = val.replace(".", "\\.")
    elif val.startswith('"') and val.endswith('"'):
        val = '"'+val[1:-1].replace('""', '\\"')+'"'
        # val = val.replace(".", "\\.")
    elif val.startswith('`') and val.endswith('`'):
        val = "'" + val[1:-1].replace("``", "`") + "'"
    elif val.startswith("+"):
        val = val[1:]
    un = ast.literal_eval(val)
    return un


def to_string(instring, tokensStart, retTokens):
    val = retTokens[0]
    val = "'"+val[1:-1].replace("''", "\\'")+"'"
    return {"literal": ast.literal_eval(val)}


# NUMBERS
realNum = Regex(
    r"[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?").addParseAction(unquote)
intNum = Regex(r"[+-]?\d+([eE]\+?\d+)?").addParseAction(unquote)

# STRINGS, NUMBERS, VARIABLES
sqlString = Regex(r"\'(\'\'|\\.|[^'])*\'").addParseAction(to_string)
identString = Regex(r'\"(\"\"|\\.|[^"])*\"').addParseAction(unquote)
mysqlidentString = Regex(r'\`(\`\`|\\.|[^`])*\`').addParseAction(unquote)
ident = Combine(~RESERVED + (delimitedList(Literal("*") | Word(alphas + "_", alphanums + "_$")
                                           | identString | mysqlidentString, delim=".", combine=True))).setName("identifier")

# EXPRESSIONS
expr = Forward()

# CASE
case = (
    CASE +
    Group(ZeroOrMore((WHEN + expr("when") + THEN + expr("then")).addParseAction(to_when_call)))("case") +
    Optional(ELSE + expr("else")) +
    END
).addParseAction(to_case_call)

selectStmt = Forward()
compound = (
    (Keyword("not", caseless=True)("op").setDebugActions(*debug) + expr("params")).addParseAction(to_json_call) |
    (Keyword("distinct", caseless=True)("op").setDebugActions(*debug) + expr("params")).addParseAction(to_json_call) |
    Keyword("null", caseless=True).setName("null").setDebugActions(*debug) |
    case |
    (Literal("(").setDebugActions(*debug).suppress() + selectStmt + Literal(")").suppress()) |
    (Literal("(").setDebugActions(*debug).suppress() + Group(delimitedList(expr)) + Literal(")").suppress()) |
    realNum.setName("float").setDebugActions(*debug) |
    intNum.setName("int").setDebugActions(*debug) |
    (Literal("-")("op").setDebugActions(*debug) + expr("params")).addParseAction(to_json_call) |
    sqlString.setName("string").setDebugActions(*debug) |
    (
        Word(alphas)("op").setName("function name").setDebugActions(*debug) +
        Literal("(").setName("func_param").setDebugActions(*debug) +
        Optional(selectStmt | Group(delimitedList(expr)))("params") +
        ")"
    ).addParseAction(to_json_call).setDebugActions(*debug) |
    ident.copy().setName("variable").setDebugActions(*debug)
)
expr << Group(infixNotation(
    compound,
    [
        (
            o,
            3 if isinstance(o, tuple) else 2,
            opAssoc.LEFT,
            to_json_operator
        )
        for o in KNOWN_OPS
    ]+[
        (
            COLLATENOCASE,
            1,
            opAssoc.LEFT,
            to_json_operator
        )
    ]
).setName("expression").setDebugActions(*debug))

# SQL STATEMENT
selectColumn = Group(
    Group(expr).setName("expression1")("value").setDebugActions(*debug) + Optional(Optional(AS) + ident.copy().setName("column_name1")("name").setDebugActions(*debug)) |
    Literal('*')("value").setDebugActions(*debug)
).setName("column").addParseAction(to_select_call)


table_source = (
    (
        (
            Literal("(").setDebugActions(*debug).suppress() +
            selectStmt +
            Literal(")").setDebugActions(*debug).suppress()
        ).setName("table source").setDebugActions(*debug)
    )("value") +
    Optional(
        Optional(AS) +
        ident("name").setName("table alias").setDebugActions(*debug)
    )
    |
    (
        ident("value").setName("table name").setDebugActions(*debug) +
        Optional(AS) +
        ident("name").setName("table alias").setDebugActions(*debug)
    )
    |
    ident.setName("table name").setDebugActions(*debug)
)

join = (
    (CROSSJOIN | FULLJOIN | FULLOUTERJOIN | INNERJOIN | JOIN | LEFTJOIN | LEFTOUTERJOIN | RIGHTJOIN | RIGHTOUTERJOIN)("op") +
    Group(table_source)("join") +
    Optional((ON + expr("on")) | (USING + expr("using")))
).addParseAction(to_join_call)

sortColumn = expr("value").setName("sort1").setDebugActions(*debug) + Optional(
    DESC("sort") | ASC("sort")) | expr("value").setName("sort2").setDebugActions(*debug)

time_interval = Group(  TIME.suppress().setDebugActions(*debug) ).addParseAction(to_time_call)

to_app = expr("value").setName("app").setDebugActions(*debug)

# define SQL tokens
selectStmt <<= delimitedList(
    #delimitedList(
        Group(
            Group(
                Group(
                    delimitedList(
                        Group(
                            SELECT.suppress().setDebugActions(*debug) + delimitedList(selectColumn)("select") +
                            Optional(TIME.suppress().setDebugActions(*debug) + expr("time"))+
                            Optional(
                                (FROM.suppress().setDebugActions(*debug) + delimitedList(Group(table_source)) + ZeroOrMore(join))("from") +
                                Optional(WHERE.suppress().setDebugActions(*debug) + expr.setName("where"))("where") +
                                Optional(GROUPBY.suppress().setDebugActions(*debug) + delimitedList(Group(selectColumn))("groupby").setName("groupby")) +
                                Optional(HAVING.suppress().setDebugActions(*debug) + expr("having").setName("having")) +
                                Optional(LIMIT.suppress().setDebugActions(*debug) + expr("limit")) +
                                Optional(OFFSET.suppress().setDebugActions(*debug) + expr("offset")) +
                                Optional(ADD.suppress().setDebugActions(*debug) + expr("add")) +
                                Optional(AVG.suppress().setDebugActions(*debug) + expr("avg")) +
                                Optional(DIVIDE.suppress().setDebugActions(*debug) + expr("divide")) +
                                Optional(MINUS.suppress().setDebugActions(*debug) + expr("minus")) +
                                Optional(MULTIPLY.suppress().setDebugActions(*debug) + expr("multiply")) +
                                Optional(TOP.suppress().setDebugActions(
                                    *debug) + expr("top"))
                            )
                        ), delim=(UNION | UNIONALL)
                    )
                )("union"))("from") +
            Optional(ORDERBY.suppress().setDebugActions(*debug) + delimitedList(Group(sortColumn))("orderby").setName("orderby")) +
            Optional(LIMIT.suppress().setDebugActions(*debug) + expr("limit")) +
            Optional(OFFSET.suppress().setDebugActions(*debug) + expr("offset"))
            + Optional(TIME.suppress().setDebugActions(*debug) + expr("time")) 
        ).addParseAction(to_union_call)
        #+ Optional ( Optional(TIME.suppress().setDebugActions(*debug) + expr("multiply")).addParseAction(to_time_call) ))   # add action
    + Optional( Optional(TO.suppress().setDebugActions(*debug) + expr.setName("to")).addParseAction(to_to_call)  ) )  # add action


SQLParser = selectStmt

# IGNORE SOME COMMENTS
oracleSqlComment = Literal("--") + restOfLine
mySqlComment = Literal("#") + restOfLine
SQLParser.ignore(oracleSqlComment | mySqlComment)
