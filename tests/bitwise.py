#!/usr/bin/env python
#from pyparsing import Word, FollowedBy,Suppress,delimitedList, Optional, Combine, Group, alphas, alphanums, Forward, restOfLine, Keyword, Literal, ParserElement, infixNotation, opAssoc, Regex, MatchFirst, ZeroOrMore
from pyparsing import *

ident = Word(alphas, alphanums)
num = Word(nums)
func = Forward()
term = ident | num | Group('(' + func + ')')
print("ident::", ident)
print("num::", num)
#print("term::", term)

print("term::", term)
func <<= ident + Group(Optional(delimitedList(term)))


result = func.parseString("fna a,b,(fnb c,d,200),100, (fnc e,f,g),300")
result.pprint(width=40)
