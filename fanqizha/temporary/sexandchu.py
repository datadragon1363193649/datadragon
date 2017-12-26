def sexandc_11_15():
    s_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/testcall_11'
    out_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/testcallupdate_11'
    sex_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/sex.txt'
    sexdic={}
    wfp=open(out_file,'w')
    with open(sex_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            sexdic[linelist[0]]=linelist[1]

    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            if linelist[0] in sexdic:
                linelist[1]=sexdic[linelist[0]]
            print linelist[34],linelist[35],linelist[36],linelist[37]

            if int(linelist[34])<0 or int(linelist[36])<0:
                continue
            r1=1.0*int(linelist[35])/(int(linelist[34])+1)
            linelist[35]=str(r1)
            r2=1.0*int(linelist[37])/(int(linelist[36])+1)
            linelist[37] = str(r2)
            wfp.write('\t'.join(linelist)+'\n')
def sexandc_30():
    s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/testcall_30_other'
    out_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/testcallupdate_30_other'
    sex_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/sexother.txt'
    sexdic = {}
    wfp = open(out_file, 'w')
    with open(sex_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            sexdic[linelist[0]] = linelist[1]

    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            if linelist[0] in sexdic:
                linelist[1] = sexdic[linelist[0]]
            print linelist[34], linelist[35], linelist[36], linelist[37]

            if int(linelist[34]) < 0 or int(linelist[36]) < 0:
                continue
            r1 = 1.0 * int(linelist[35]) / (int(linelist[34]) + 1)
            linelist[35] = str(r1)
            r2 = 1.0 * int(linelist[37]) / (int(linelist[36]) + 1)
            linelist[37] = str(r2)
            wfp.write('\t'.join(linelist) + '\n')
if __name__ == '__main__':
    sexandc_30()