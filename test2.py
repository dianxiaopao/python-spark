#!/usr/bin/python
# URL that generated this code:
# http://www.txt2re.com/index-python.php3?s=jdbc:mysql://192.168.1.200:3307/bd_ets?user=root%26password=13851687968%26useUnicode=true%26characterEncoding=UTF-8%26zeroDateTimeBehavior=convertToNull%20jdbc:mysql://192.168.1.200:3306/osce1030?user=root%26password=misrobot_whu%26useUnicode=true%26characterEncoding=UTF-8%26zeroDateTimeBehavior=convertToNull&-48

import re

txt='jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull jdbc:mysql://192.168.1.200:3306/osce1030?user=root&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull'

re1='.*?'	# Non-greedy match on filler
re2='(root)'	# Word 1

rg = re.compile(re1+re2,re.IGNORECASE|re.DOTALL)
m = rg.search(txt)
if m:
    word1=m.group(1)
    print "("+word1+")"+"\n"

#-----
# Paste the code into a new python file. Then in Unix:'
# $ python x.py
#-----