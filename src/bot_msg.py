from typing import Callable


## HELPER FUNCTIONS
# -----

def trim_msg(func) -> Callable:
    def wrap(*args, **kwargs):
        msg = func(*args, **kwargs)
        
        return msg.replace("    ", "").strip()
    
    return wrap


## TELEGRAM BOT MESSAGE FUNCTIONS
# -----

@trim_msg
def get_msg_start(lang:str, name:str, tier:str) -> str:
    if lang == "zh":
        if tier == "FIRST_TIME":
            msg = f"""
            哈囉{name}！我喺你隻超市看門狗啊！
            
            我會幫你昅實超市啲優惠，同埋我會同提你幾時可以入手平價貨。
            
            一齊嚟屠宰啲超市喇！
            
            [聲明] 你Telegram啲「用戶身份」同「設定語言」會被我收集嚟用。
            """
        elif tier == "REPEAT":
            msg = f"""
            又 /start 乜春嘢啊{name}？你知唔知我要打依段嘢好攰好辛苦？
            """
        
        msg += """
        [用戶指南]
        =====
        /view [structural number] \u27ad 睇吓有啲貨係可以昅住
        /plot [sku] \u27ad 睇吓件貨嗰價格趨勢
        
        /add [sku ...] \u27ad 放啲想買嘅嘢入追蹤表
        /rm [sku ...] \u27ad 拎走啲唔想關注嘅嘢
        /list \u27ad 睇吓個追蹤表有啲乜
        
        /watch \u27ad 隨機抽5個今日抵買嘅嘢睇吓
        /sub \u27ad 訂閲每日價格通知
        /unsub \u27ad 取消訂閲價格通知
        
        /lang \u27ad 讕喺嘢轉做英文
        """
    elif lang == "en":
        if tier == "FIRST_TIME":
            msg = f"""
            Good day {name}! I'm your SuperPriceWatchdog!
            
            I always keep my eyes on supermarket promotions and \
            I'll alert you whenever your favourite items are on fantastic deals.
            
            Let's start our savings journey!
            
            [DISCLAIMER] your Telegram `user ID` and `Language` will be collected.
            """
        elif tier == "REPEAT":
            msg = f"""
            How come you /start again, {name}? \
            Don't you know it take time and effort to type everything again?
            """
        
        msg += """
        [USER GUIDE]
        =====
        /view [structural number] \u27ad check out traceable items
        /plot [sku] \u27ad show the price trend of an item
        
        /add [sku ...] \u27ad add item(s) to the tracking list
        /rm [sku ...] \u27ad remove item(s) from the tracking list
        /list \u27ad display tracking list
        
        /watch \u27ad check 5 random items on price deals
        /sub \u27ad subscribe to receive daily pricing alert
        /unsub \u27ad stop receiving alert from me
        
        /lang \u27ad switch to Traditional Chinese
        """
    
    return msg


@trim_msg
def get_msg_view(lang:str, items:dict, tier:str) -> str:
    idx_dept, _ = items["idx"]
    dept, cat = items["struct"]
    item_lst = "\n".join(items["item_lst"])
    
    if lang == "zh":
        if item_lst != "" and tier in ["DEPARTMENT", "CATEGORY", "ITEM"]:
            msg = f"""
            [物品清單]
            =====
            母組別:     {dept}
            子組別:     {cat}
            =====
            {item_lst}
            =====
            """
            
            if tier == "DEPARTMENT":
                msg += """
                輸入 /view D[數字]睇更多。
                """
            elif tier == "CATEGORY":
                msg += f"""
                輸入 /view D{idx_dept}C[數字]睇更多。
                """
            elif tier == "ITEM":
                msg += """
                輸入 /add P[代碼]加入去你個追蹤表。
                """
        else:
            if tier == "UNKNOWN":
                msg = """
                錯誤參數。唔該你跟返個格式打。
                [格式] D[數字]C[數字]
                """
            else:
                msg = """
                錯誤參數。你個參數唔喺範圍之內。
                """
    elif lang == "en":
        if item_lst != "" and tier in ["DEPARTMENT", "CATEGORY", "ITEM"]:
            msg = f"""
            [ITEM LIST]
            =====
            Group:     {dept}
            SubGroup:  {cat}
            =====
            {item_lst}
            """
            
            if tier == "DEPARTMENT":
                msg += """
                Type /view D[number] to find out more.
                """
            elif tier == "CATEGORY":
                msg += f"""
                Type /view D{idx_dept}C[number] to find out more.
                """
            elif tier == "ITEM":
                msg += """
                Type /add P[code] to add to your tracking list.
                """
        else:
            if tier == "UNKNOWN":
                msg = """
                Invalid input. \
                Please type in the correct format.
                [FORMAT] D[number]C[number]
                """
            else:
                msg = """
                Invalid input. \
                Your input is out of range.
                """
    
    return msg


@trim_msg
def get_msg_plot(lang:str, tier:str) -> str:
    if lang == "zh":
        if tier == "GRAPH":
            msg = """
            下面就係個價格趨勢。
            """
        elif tier == "INVALID":
            msg = """
            錯誤參數。唔該你打返個啱嘅代碼。你可以用 /view 搵相關嘅貨品代碼。
            """
        elif tier == "UNKNOWN":
            msg = """
            [用戶指南]
            =====
            睇吓件貨嗰價格趨勢。你每次只可以睇一件貨。
            
            [例子 01] /plot P000000002
            """
    elif lang == "en":
        if tier == "GRAPH":
            msg = """
            See the price trend below.
            """
        elif tier == "INVALID":
            msg = """
            Invalid input. \
            Please type in the correct product code. \
            You can find the code by typing /view.
            """
        elif tier == "UNKNOWN":
            msg = """
            [USER GUIDE]
            =====
            Show the price trend of an item. \
            You can only view one item per call.
            
            [EXAMPLE 01] /plot P000000002
            """
    
    return msg


@trim_msg
def get_msg_add(lang:str, cnt:int, tier:str) -> str:
    if lang == "zh":
        if tier == "ADD":
            msg = f"""
            放咗{cnt}件貨入個追蹤表到。
            """
        elif tier == "INVALID":
            msg = """
            錯誤參數。唔該你打返個啱嘅代碼。你可以用 /view 搵相關嘅貨品代碼。
            """
        elif tier == "EXIST":
            msg = """
            係咪玩嘢啊？你之前咪放咗入去囉。
            """
        elif tier == "UNKNOWN":
            msg = """
            [用戶指南]
            =====
            放啲想買嘅嘢入追蹤表。你只可以放啲啱嘅代碼同未追蹤嘅貨品入個表到。
            
            [例子 01] /add P000000002
            [例子 02] /add P000000002 P000000003
            """
    elif lang == "en":
        if tier == "ADD":
            msg = f"""
            Added {cnt} item(s) to the tracking list.
            """
        elif tier == "INVALID":
            msg = """
            Invalid input. \
            Please type in the correct product code. \
            You can find the code by typing /view.
            """
        elif tier == "EXIST":
            msg = """
            Item(s) has/have been added to the list before.
            """
        elif tier == "UNKNOWN":
            msg = """
            [USER GUIDE]
            =====
            Add item(s) to the tracking list. \
            You can only add valid item(s) that aren't already being tracked.
            
            [EXAMPLE 01] /add P000000002
            [EXAMPLE 02] /add P000000002 P000000003
            """
    
    return msg


@trim_msg
def get_msg_rm(lang:str, cnt:int, tier:str) -> str:
    if lang == "zh":
        if tier == "REMOVE":
            msg = f"""
            拎走咗{cnt}件貨喺個追蹤表。
            """
        elif tier == "INVALID":
            msg = """
            錯誤參數。唔該你打返個啱嘅代碼。你可以用 /list 搵相關嘅貨品代碼。
            """
        elif tier == "ABSENT":
            msg = """
            你失咗憶？個追蹤表都無件貨點拎走？
            """
        elif tier == "EMPTY":
            msg = """
            幫你清咗個追蹤表。
            """
        elif tier == "UNKNOWN":
            msg = """
            [USER GUIDE]
            =====
            拎走啲唔想關注嘅嘢。你只可以拎走啲啱嘅代碼同追蹤緊嘅貨品。
            
            [例子 01] /rm P000000002
            [例子 02] /rm P000000002 P000000003
            [例子 03] /rm ALL
            """
    elif lang == "en":
        if tier == "REMOVE":
            msg = f"""
            Removed {cnt} item(s) from the tracking list.
            """
        elif tier == "INVALID":
            msg = """
            Invalid input. \
            Please type in the correct product code. \
            You can find the code by typing /list.
            """
        elif tier == "ABSENT":
            msg = """
            The list don't have such item(s).
            """
        elif tier == "EMPTY":
            msg = """
            Removed all items from the list.
            """
        elif tier == "UNKNOWN":
            msg = """
            [USER GUIDE]
            =====
            Remove item(s) from the tracking list. \
            You can only remove valid item(s) that are already being tracked.
            
            [EXAMPLE 01] /rm P000000002
            [EXAMPLE 02] /rm P000000002 P000000003
            [EXAMPLE 03] /rm ALL
            """
    
    return msg


@trim_msg
def get_msg_list(lang:str, items:list) -> str:
    item_str = "\n".join(f"\u2713 {item}" for item in items)
    cnt = len(items)
    
    if lang == "zh":
        msg = f"""
        [追蹤表]
        {item_str}
        
        一共追蹤緊{cnt}件貨。
        """
    elif lang == "en":
        msg = f"""
        [Tracking List]
        {item_str}
        
        Total of {cnt} under tracking.
        """
    
    return msg


@trim_msg
def get_msg_watchdog(lang:str, watch_lst:list, tier:str) -> str:
    if lang == "zh":
        if tier == "WATCHDOG":
            msg = ""
            for sku in watch_lst:
                msg += f"""
                貨品: {sku["brand"]}; {sku["name"]}
                每件價格: ${sku["price"]:.2f} ({sku["promo"]})
                超級市場: {sku["smkt"]}
                MA({sku["freq"]:.0f}) 統計: \
                平均. ${sku["price_avg"]:.2f} ± {sku["price_std"]:.2f} | \
                最低. ${sku["price_q0"]:.2f} | \
                最高. ${sku["price_q4"]:.2f}
                """
        elif tier == "INVALID":
            msg = """
            今日無嘢值得買。
            """
    elif lang == "en":
        if tier == "WATCHDOG":
            msg = ""
            for sku in watch_lst:
                msg += f"""
                Product: {sku["brand"]}; {sku["name"]}
                Unit Price: ${sku["price"]:.2f} ({sku["promo"]})
                Supermarket: {sku["smkt"]}
                MA({sku["freq"]:.0f}) Statistics: \
                avg. ${sku["price_avg"]:.2f} ± {sku["price_std"]:.2f} | \
                min. ${sku["price_q0"]:.2f} | \
                max. ${sku["price_q4"]:.2f}
                """
        elif tier == "INVALID":
            msg = """
            No item is worth buying today.
            """
    
    return msg


@trim_msg
def get_msg_sub(lang:str, tier:str) -> str:
    if lang == "zh":
        if tier == "SUBSCRIBE":
            msg = """
            汪！係時候屠宰啲超市喇！每日 12:30 我就會吠醒你最抵買嘅平價貨！
            """
        elif tier == "INVALID":
            msg = """
            我已經幫緊你睇實啲優惠。每日 12:30 我都會吠醒你。
            """
    elif lang == "en":
        if tier == "SUBSCRIBE":
            msg = """
            Wooo! Let's embark on our saving journey! \
            Every day at 12:30, I'll send you the latest deals!
            """
        elif tier == "INVALID":
            msg = """
            You have subscribed before. \
            You'll receive notice every day at 12:30.
            """
    
    return msg


@trim_msg
def get_msg_unsub(lang:str, tier:str) -> str:
    if lang == "zh":
        if tier == "UNSUBSCRIBE":
            msg = """
            遺棄你隻看門狗小心俾愛協打 ˙◠˙
            """
        elif tier == "INVALID":
            msg = """
            你都無訂閲價格通知點幫你取消？
            """
    elif lang == "en":
        if tier == "UNSUBSCRIBE":
            msg = """
            You have been unsubscribed from the price alert ˙◠˙
            """
        elif tier == "INVALID":
            msg = """
            You haven't subscribed to the price alert yet.
            """
    
    return msg


@trim_msg
def get_msg_lang(lang:str) -> str:
    if lang == "zh":
        msg = """
        幫你轉咗做中文。
        """
    elif lang == "en":
        msg = """
        Changed to English language.
        """
    
    return msg


@trim_msg
def get_msg_bye(lang:str) -> str:
    if lang == "zh":
        msg = """
        咁你都知有依樣嘢喎。你要再 /start 我先幫到你。
        """
    elif lang == "en":
        msg = """
        You know the secret! \
        You need to /start again to resume everything.
        """
    
    return msg


@trim_msg
def get_msg_unknown(lang:str) -> str:
    if lang == "zh":
        msg = """
        唔知你噏乜嘢。
        """
    elif lang == "en":
        msg = """
        I don't understand what you're talking about.
        """
    
    return msg


@trim_msg
def get_msg_reply(lang:str) -> str:
    if lang == "zh":
        msg = """
        你可以用 /start 睇有啲乜功能。
        """
    elif lang == "en":
        msg = """
        Type /start to view command.
        """
    
    return msg
