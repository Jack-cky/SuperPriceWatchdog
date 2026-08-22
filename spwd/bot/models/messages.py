class BotMessages:
    @classmethod
    def start(cls, usr_name: str, language: str, status: str, msg: str) -> str:
        return {
            ("en", "new"): (
                "Good day {}! I'm SuperPriceWatchdog!\n\n"
                "I always keep my eyes on supermarket promotions and "
                "I'll alert you whenever your favourite items are on "
                "fantastic deals.\n\n"
                "Let's start our savings journey! 🐶\n"
                "Use /start to continue."
            ),
            ("en", "repeat"): (
                "How come you /start again, {}? Use /help for the "
                "user guide."
            ),
            ("zh", "new"): (
                "哈囉{}！我喺超市看門狗啊！\n\n"
                "我會幫你昅實超市啲優惠，同埋我會同提你幾時可以入手平價貨。\n\n"
                "一齊嚟屠宰啲超市喇！ 🐶\n"
                "用 /start 繼續。"
            ),
            ("zh", "repeat"): (
                "你又 /start 乜春嘢啊{}？唔知點用就 /help 喇。"
            ),
        }.get((language, status), msg).format(usr_name)

    @classmethod
    def help(cls, language: str, msg: str) -> str:
        return {
            "en": (
                "🔰 Helping you, helping you\n\n"
                "Go to this "
                "<a href='https://online-price-watch.consumer.org.hk/opw/"
                "category'>website</a> to find an item you're interested in, "
                "then copy the link and send it to me. I'll help you keep "
                "track of the price trends for that item.\n\n"
                "Easy peasy! Suppose you want to buy a specific type of soft "
                "drink, just send me the link. Try sending me this link:\n"
                "https://online-price-watch.consumer.org.hk/opw/"
                "product/P000000002\n"
                "If you want to stop monitoring it, just send me the link "
                "again.\n\n"
                "I'll add the product you want to track to this /list, and "
                "you can view and modify the items in it anytime. Remember "
                "to /sub for daily price alerts, so when the item is at a "
                "good price, I'll remind you to buy it.\n\n"
                "If you feel a bit lost, you can try feeling /lucky and "
                "randomly see what's on sale today. Also, you can 更改 your "
                " /lang to 中文 if you 唔識睇.\n\n"
                "If you want to give feedback or find any bugs, you can "
                "leave a message for us to improve at this "
                "<a href='https://github.com/Jack-cky/"
                "SuperPriceWatchdog/discussions'>link</a>."
            ),
            "zh": (
                "🔰 幫緊你，幫緊你\n\n"
                "去依個"
                "<a href='https://online-price-watch.consumer.org.hk/opw/"
                "category'>網站</a>到搵你想關注嘅產品，然後copy條link俾我，"
                "我就會幫你留意件貨嘅價錢趨勢㗎喇。\n\n"
                "好簡單！譬如你想飲某牌子嘅汽水，咁你只需要搵條到link俾我就得㗎喇。"
                "試下copy依條link俾我：\n"
                "https://online-price-watch.consumer.org.hk/opw/"
                "product/P000000002\n"
                "如果唔想我再留意件貨，send多次條link俾我就得㗎喇。\n\n"
                "我會將你想關注嘅產品放入依條 /list 裏面，你隨時都可以"
                "睇返同修改入面嘅嘢。記住 /sub 每日嘅價格通知，咁到時件貨抵買嘅"
                "時間我就會提你入手㗎喇。\n\n"
                "如果你覺得迷惘，你可以試下 feeling /lucky 咁隨機睇吓今日有啲"
                "乜嘢抵買。仲有you可以讕喺嘢咁change你個 /lang 去English。\n\n"
                "如果你想發表意見或者發現有bugs，你可以去"
                "<a href='https://github.com/Jack-cky/"
                "SuperPriceWatchdog/discussions'>依到</a>留個言俾我地去改善㗎。"
            ),
        }.get(language, msg)

    @classmethod
    def sub(cls, language: str, status: str, msg: str) -> str:
        return {
            ("en", True): (
                "Woof! Let's embark on our saving journey! Every day "
                "(except Friday and Saturday) around <u><b>12:00</b></u>, "
                "I'll bark you the latest deals!"
            ),
            ("en", False): "Sad to see you go.",
            ("zh", True): (
                "汪！係時候屠宰啲超市喇！每日(除週五六)大概"
                "<u><b>12:00</b></u>我就會吠醒你最抵買嘅平價貨！"
            ),
            ("zh", False): "遺棄看門狗因住俾愛協打。",
        }.get((language, status), msg)

    @classmethod
    def lucky(cls, language: str, msg: str) -> str:
        return {
            "en": (
                "🍀 🍀 🍀 🍀 🍀\n\n"
                "Data is being updated. It's not lucky on Fridays "
                "and Saturday."
            ),
            "zh": (
                "🍀 🍀 🍀 🍀 🍀\n\n"
                "數據更新中，逢週五六唔lucky。"
            )
        }.get(language, msg)

    @classmethod
    def lang(cls, language: str, msg: str) -> str:
        return {
            "en": "Changed to English language.",
            "zh": "幫你轉咗做中文。",
        }.get(language, msg)

    @classmethod
    def edit(cls, language: str, status: str, valid: bool, msg: str) -> str:
        return {
            ("en", "add", True): "Roger! I'm staring at this item.",
            ("en", "remove", True): "Sure! I'll no long monitor this item.",
            ("en", "na", False): "No this item, dude. Check it again.",
            ("zh", "add", True): "收到！會睇實依件貨啲價格趨勢。",
            ("zh", "remove", True): "可以！唔會再留意依件貨。",
            ("zh", "na", False): "無依件貨㗎喎，睇清楚啲。",
        }.get((language, status, valid), msg)

    @classmethod
    def error(cls, status: str, msg: str) -> str:
        return {
            "pipeline": (
                "[Error 500] Internal Error. Data Pipeline appeared to "
                "have issues."
            ),
            "user": (
                "[Error 500] Internal Error. It has been recorded in the "
                "system log.\n\n"
                "If the problem persists, please create an issue at "
                "<a href='https://github.com/Jack-cky/"
                "SuperPriceWatchdog/issues'>here</a>."
            ),
        }.get(status, msg)

    @classmethod
    def bye(cls, language: str, msg: str) -> str:
        return {
            "en": "You know the secret! /start again to restart everything.",
            "zh": "咁你都知有依樣嘢喎。你要 /start 多次我先幫到你。",
        }.get(language, msg)

    @classmethod
    def unk(cls, status: str) -> str:
        return {
            "unk": "🐶 bow-wow 汪 ~ 🐶 bow-wow 汪 ~",
            "na": "🚬 ૮´˶• ᴥ •˶`ა 🥃",
        }.get(status, "Error 404. 🦴")
