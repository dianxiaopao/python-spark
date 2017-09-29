
def get_newlist(zxxl_ds_1, znsb_ds_2, ksmxr_ds_3, ets_apply_student_ds_4,ets_osce_score_group_ds_5):
    list = [zxxl_ds_1, znsb_ds_2, ksmxr_ds_3, ets_apply_student_ds_4, ets_osce_score_group_ds_5]
    newlists = []
    for index1, a in enumerate(list):
        for index2, b in enumerate(list):
            for index3, c in enumerate(list):
                if (a != b != c  and index3 > index2 > index1):
                    temlists = []
                    temlists.append(a)
                    temlists.append(b)
                    temlists.append(c)
                    newlists.append(temlists)
                    for index4, d in enumerate(list):
                        if (a != b != c!= d and index4 >index3 > index2 > index1):
                            temlists = []
                            temlists.append(a)
                            temlists.append(b)
                            temlists.append(c)
                            temlists.append(d)
                            newlists.append(temlists)
                        for index5, e in enumerate(list):
                            if (a != b!=c!=d!=e and index5 > index4 >index3 > index2> index1):
                                temlists = []
                                temlists.append(a)
                                temlists.append(b)
                                temlists.append(c)
                                temlists.append(d)
                                temlists.append(e)
                                newlists.append(temlists)
    dslist = []

    def dsjoin(x, y):
        return x.join(y, 'operatorstudentid')

    for dss in newlists:
        dslist.append(reduce(dsjoin, dss))
    return dslist