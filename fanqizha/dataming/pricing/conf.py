# -*- encoding: utf-8 -*-
# grade_num=2
grade_list=['good','common']
data_cost=200 #数据费用
user_num=1000 #认证用户
# pass_rate=0.02
# 呆帐率
# 利率
# 服务费
# 等级通过率
# 贷款金额
# 贷款天数
good={'bad_debt':0.05,
      'interest_rate':0.003,
      'service_money':250,
      'level':0.2,
      'loan_amount':2000,
      'loan_days':30
      }

common={'bad_debt':0.1,
      'interest_rate':0.006,
      'service_money':300,
     'level':0.5,
     'loan_amount':2000,
     'loan_days':30
      }
# 通过率变化度
fineness=5