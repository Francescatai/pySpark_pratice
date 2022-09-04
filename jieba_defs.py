import jieba

# 用jieba進行分詞操作
def context_jieba(data):
    newdata = jieba.cut_for_search(data)
    l = list()
    for word in newdata:
        l.append(word)
    return l

# 過濾停用詞
def filter_words(data):
    return data not in ['谷','幫','客']

# 修訂關鍵詞
def append_words(data):
    if data == '傳':data = '傳智'
    return (data,1)

# 處理用戶搜尋內容數據(user_id,search_content)
def extract_user_and_word(data):
    user_id = data[0]
    search_content = [1]
#     對search_content進行分詞
    words = context_jieba(search_content)
    l = list()
    for word in words:
        if filter_words(word):
            l.append((user_id+"-"+append_words(word)[0],1))
    return l

