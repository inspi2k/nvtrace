import gspread
import sys
import urllib.request
import time
import json
import ssl
import re
import datetime
import warnings

ssl._create_default_https_context = ssl._create_unverified_context

# Global Variables
gs_json = 'cglab-python-9750d891fb6e.json'
gs_url = 'https://docs.google.com/spreadsheets/d/18O363rcAZY7bz6l7BAjCcekyx08Ytv1Ana1LLtO8Uhs'
gs_sheet_trace_list = 'trace_list'
gs_sheet_trace_rank = 'trace_rank'

# Naver Search API id, secret key (cg-lab)
CLIENT_ID = '4L9CRB_dZ8HKe4R1WmNL'
CLIENT_SECRET = 'HLoLebsBkd'

print()

# 1. 구글 시트를 읽어서 MID - 키워드 리스트를 가져옴
# output - list_mid[], list_keyword[][]
try:

  gc = gspread.service_account(filename='./'+gs_json)
  doc = gc.open_by_url(gs_url)
  worksheet = doc.worksheet(gs_sheet_trace_list)

  list_mid = []
  list_title = []
  list_store = []
  list_keyword = []
  list_keyword_num = []
  list_rank = []

  column_no = -1
  while True:
    column_no += 2

    items = worksheet.col_values(column_no)

    if len(items) < 3: break # 읽어온 줄에 값이 없을 경우 (MID만 있고 keyword 없어도 검색 안 함)

    items_num = worksheet.col_values(column_no+1)

    if len(items) != len(items_num):  # 중간에 검색량이 비어있으면 괜찮지만 마지막 셀이 비어있으면 차이가 남
      for i in range(len(items_num), len(items)):
        items_num.insert(i, '')

    list_mid.append(items[1]) # row(2) - MID
    # list_keyword.append(list(map(str, items[2:]))) # row(3)~ - keywords
    # list_keyword_num.append(list(map(str, items_num[2:])))
    lk = []
    lk_num = []
    for v in range(2, len(items[2:])+2):
      if items[v].strip() != '':
        lk.append(items[v].strip())
        lk_num.append(items_num[v].strip())
    list_keyword.append(lk)
    list_keyword_num.append(lk_num)

except gspread.exceptions.APIError as e:
  print('No More Columns on Sheet\n')

except Exception as e:
  print('Sheet Processing Exception:',e)
  sys.exit(1)

print(f'OK / Reading Data - {len(list_mid)} MIDs')

# console view - list
# print('-----------')
# for i in range(len(list_mid)):  # for key in list_keyword:
#   print(list_mid[i], end=' ')
#   print(f'[{len(list_keyword[i]):>2}]', end=' ')
#   for keyword in list_keyword[i]: # for keyword in key:
#     print(keyword, end=' ')
#   print()
# print('-----------')

# 2. open api - search
try:
  mid_count = 0
  while mid_count < len(list_mid):            # MID 마다 반복

    print(f'\n{list_mid[mid_count]}', end='', flush=True)
    print_title = False

    # for keyword in list_keyword[mid_count]:   # Keyword 마다 반복
    for k in range(len(list_keyword[mid_count])):
      keyword = list_keyword[mid_count][k]

      list_rank.append([])    # keyword의 rank 저장할 리스트 생성
      # list_rank.insert(mid_count,[])  #append로 생성해야 2차원 배열이 됨

      encText = urllib.parse.quote(keyword)

      pm_start = 1
      found_it = False
      long_sleep = False  # 많은 요청을 줄이는 용도 확인용 변수

      while (found_it == False) and (pm_start <= 1000):
        pm_display = 99 if pm_start == 1 else 100
        # if pm_start == 1: pm_display = 99
        # else: pm_display = 100
        
        # api limit per second
        if (pm_start % 100) == 0:
            print(',', end='', flush=True)
            time.sleep(0.1)
            if (pm_start > 500) and (long_sleep == False):
              long_sleep = True
              print('_', end='', flush=True)
              time.sleep(1)

        # nv open api
        url = (f'https://openapi.naver.com/v1/search/shop?start={pm_start}&display={pm_display}&query={encText}')

        request = urllib.request.Request(url)
        request.add_header('X-Naver-Client-Id', CLIENT_ID)
        request.add_header('X-Naver-Client-Secret', CLIENT_SECRET)
        response = urllib.request.urlopen(request)
        rescode = response.getcode()
        if rescode == 200:
          response_body = response.read()
          # print(f's={pm_start:<4}d={pm_display:<3} ', end='', flush=True)
          # print('.', end='', flush=True)
        else:
          print('Error Code:' + rescode)
          sys.exit(1)

        data = json.loads(response_body.decode('utf-8'))  # JSON 형태의 문자열 읽기

        if data['total'] == 0: break  # total 이 1,000개 미만 짜리 처리

        for prd in data['items']:
          if list_mid[mid_count] == prd['productId']:
            list_rank[mid_count].append(pm_start)
            # print('_', end='', flush=True)
            # num_page = (pm_start - 1) // 40 + 1
            # num_ppos = (pm_start - 1) % 40 + 1
            # print(f"{list_mid[mid_count]} {re.sub('(<([^>]+)>)', '', prd['title'])}")
            if print_title == False:
              print(f' {re.sub("(<([^>]+)>)", "", prd["title"])}\n   [{len(list_keyword[mid_count]):>2}]', end='', flush=True)
              list_title.append(re.sub("(<([^>]+)>)", "", prd["title"]))
              list_store.append(prd['mallName'])
              print_title = True
            else:
              print('\n      ', end='', flush=True)
            print(f' {pm_start:>4}th ({((pm_start - 1) // 40 + 1):>2}p {((pm_start - 1) % 40 + 1):>2}) {keyword} ({list_keyword_num[mid_count][k]})', end='', flush=True)
            found_it = True
            break

          pm_start += 1

      if found_it == False:
        list_rank[mid_count].append('')
        print(f'\n         -over 1,099th {keyword} ({list_keyword_num[mid_count][k]})', end='', flush=True)

    mid_count += 1
    print()

except Exception as e:
  print('\nAPI Call Exception:',e)
  print(url)
  print(response_body.decode('utf-8'))
  sys.exit(1)

# console view - list
# print()
# print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
# for i in range(len(list_mid)):  # for key in list_keyword:
#   print(f'{list_mid[i]} {list_title[i]}')
#   print(f'       [{len(list_keyword[i]):>2}]', end='')
#   for j in range(len(list_keyword[i])): # for keyword in key:
#     print(f' {list_keyword[i][j]}({list_rank[i][j]})', end='')
#   print()
# print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

# 3. 구글 시트에 현재 일시로 순위 기록
try:

  worksheet = doc.worksheet(gs_sheet_trace_rank)  # 변수 재활용 read -> write

  row = len(worksheet.col_values(1)) + 1

  writing_values = []
  writing_count = 0
  writing_time = datetime.datetime.now()

  for i in range(len(list_mid)):
    for j in range(len(list_keyword[i])):
      writing_count += 1
      writing_row = []
      writing_row.append(writing_time.strftime('%Y-%m-%d'))
      writing_row.append(writing_time.strftime('%H:%M:%S'))
      writing_row.append(int(list_mid[i]))
      writing_row.append(list_store[i])
      writing_row.append(list_title[i])
      writing_row.append(list_keyword[i][j])
      writing_row.append(list_rank[i][j])
      if list_rank[i][j] == '': writing_row.append('')
      else: writing_row.append((list_rank[i][j] - 1) // 40 + 1)
      writing_values.append(writing_row)

  warnings.filterwarnings(action="ignore")
  worksheet.update(f'A{str(row)}:H{str(row + writing_count)}', writing_values)
  warnings.filterwarnings(action="default")

except Exception as e:
  print('Sheet Writing Exception:', e)
  sys.exit(1)

# end
print()
print(f'{writing_time}\tComplete Tracing - {len(list_mid)} MIDs, {writing_count} records')
print()