# -*- cocoding: utf-8 -*-
import pandas as pd
import conf
def profit(user):
    aa=1

def simulation():
    grade_list=conf.grade_list
    data_cost=conf.data_cost
    user=[]
    pass_rate_list=[]
    user_num=conf.user_num
    # 数据费用
    data_cost_all=data_cost*user_num
    for gi in grade_list:
        if gi=='good':
            user.append(conf.good)
            pass_rate_list.append(conf.good['level'])
        if gi=='common':
            user.append(conf.common)
            pass_rate_list.append(conf.common['level'])
        # profit(user)
    for pass_value in range(0,100,conf.fineness):
        # if pass_value<pass_rate_list[pi]:
        #     aa=1
        # elif pass_rate_list<pass_rate_list[pi+1]:
        #     pi+=1
        level_interest=0-data_cost_all
        pv=pass_value*1.0/100
        print pv
        if pv>pass_rate_list[-1]:
            break
        for pi in range(len(pass_rate_list)):
            if pv <=pass_rate_list[pi]:
                level_infor=user[pi]
                bad_debt=level_infor['bad_debt']
                interest_rate=level_infor['interest_rate']
                service_money=level_infor['service_money']
                loan_amount=level_infor['loan_amount']
                loan_days=level_infor['loan_days']
                # 利润收入
                profit = (loan_amount * interest_rate * loan_days) *int( (
                user_num * pv * (1 - bad_debt)) )
                # 服务费收入
                service = service_money * int((user_num * pv * (1 - bad_debt)) )
                # 信贷损失
                bad = loan_amount * int((user_num * pv * bad_debt))
                print 'pro', profit + service
                print 'bad',bad
                profits_money = profit + service - bad
                level_interest += profits_money
                print '通过率：',pass_value,'净利润：',level_interest
                break
            else:
                level_infor = user[pi]
                bad_debt = level_infor['bad_debt']
                interest_rate = level_infor['interest_rate']
                service_money = level_infor['service_money']
                loan_amount = level_infor['loan_amount']
                loan_days = level_infor['loan_days']
                # 利润收入
                profit=(loan_amount*interest_rate*loan_days)*int(
                    (user_num*pass_rate_list[pi]*(1-bad_debt)))
                # 服务费收入
                service=service_money*int((user_num*pass_rate_list[pi]*(1-bad_debt)))
                # 信贷损失
                bad=loan_amount*int((user_num*pass_rate_list[pi]*bad_debt))
                profits_money=profit+service-bad
                level_interest+=profits_money

if __name__ == '__main__':
    simulation()
