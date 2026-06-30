from datetime import datetime, timedelta
from io import BytesIO

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from flask import current_app

from ..models import BotMessages, PromotionDays

matplotlib.use("agg")


def _fmt_price(row: dict[str, float | int | str | None]) -> str:
    if abs(row["original_price"] - row["unit_price"]) <= 0.1:
        return f"${row['unit_price']:.1f}\n"
    return (
        f"<s>${row['original_price']:.1f}</s> → "
        f"${row['unit_price']:.1f} ({row['promotion']})\n"
    )


def slash_alert(usr_id: int) -> str | None:
    response = current_app.supabase_client.rpc(
        "get_alert",
        {"p_user_id": usr_id},
    ).execute()

    items = []
    for row in response.data:
        price = _fmt_price(row)

        items.append(
            f"/{row['sku']} | {row['brand']} - {row['name']}\n"
            f"{row['supermarket']} @ {price}"
        )

    today = datetime.now(current_app.hkt)
    year, week, weekday = today.isocalendar()
    dates = [
        (today + timedelta(days=delta)).day
        for delta in range((4 - weekday) % 7 + 1)
    ]

    special_offer = "".join(
        promo.get_card_offer(dates)
        for promo in PromotionDays
    )

    return f"📢 {year} WK{week} Day {(weekday + 1) % 7} 📢\n\n" + \
        "\n".join(items) + special_offer if items else None


def slash_bye(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "remove_user",
        {"p_user_id": usr_id},
    ).execute()

    return BotMessages.bye(
        response.data[0].get("language"),
        slash_unk("na"),
    )


def slash_edit(usr_id: int, code: str) -> str:
    response = current_app.supabase_client.rpc(
        "edit_watchlist",
        {"p_user_id": usr_id, "p_sku": code},
    ).execute()

    return BotMessages.edit(
        response.data[0].get("language"),
        response.data[0].get("status"),
        response.data[0].get("valid"),
        slash_unk("na"),
    )


def slash_error(status: str) -> str:
    return BotMessages.error(
        status,
        slash_unk("na"),
    )


def slash_help(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "get_language",
        {"p_user_id": usr_id},
    ).execute()

    return BotMessages.help(
        response.data[0].get("language"),
        slash_unk("na"),
    )


def slash_lang(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "change_language",
        {"p_user_id": usr_id},
    ).execute()

    return BotMessages.lang(
        response.data[0].get("language"),
        slash_unk("na"),
    )


def slash_list(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "get_watchlist",
        {"p_user_id": usr_id},
    ).execute()

    items = [
        f"/{row['sku']} | {row['brand']} - {row['name']}"
        for row in response.data
    ]

    return "🛒 🛒 🛒 🛒 🛒\n\n" + "\n".join(items) if items else slash_unk("na")


def slash_lucky(usr_id: int) -> str:
    year, week, weekday = datetime.now(current_app.hkt).isocalendar()

    if weekday not in [5, 6]:
        response = current_app.supabase_client.rpc(
            "draw_deals",
            {"p_user_id": usr_id},
        ).execute()

        items = []
        for row in response.data:
            price = _fmt_price(row)

            sku = row["sku"]
            brand = row["brand"]
            name = row["name"]
            supermarket = row["supermarket"]
            freq = row["frequency"]
            avg = row["average_price"]
            std = row["std_price"]
            q0 = row["q0_price"]
            q4 = row["q4_price"]

            item_html = (
                "<a href='https://online-price-watch.consumer.org.hk/'"
                + "opw/product/"
                + sku
                + f"'>{sku}</a> "
                + f"{brand} - {name}\n"
                + f"{supermarket} @ {price}"
                + f"<span class='tg-spoiler'>MA({freq}): "
                + f"${avg:.1f} ± {std:.1f} | min. "
                + f"${q0:.1f} | max. ${q4:.1f}</span>"
            )

            items.append(item_html)

        msg = (
            f"🍀 {year} WK{week} Day {(weekday + 1) % 7} 🍀\n\n"
            + "\n\n".join(items)
            if items
            else slash_unk("na")
        )
    else:
        response = current_app.supabase_client.rpc(
            "get_language",
            {"p_user_id": usr_id},
        ).execute()

        msg = BotMessages.lucky(
            response.data[0].get("language"),
            slash_unk("na"),
        )

    return msg


def slash_plot(usr_id: int, code: str) -> bytes:
    response = current_app.supabase_client.rpc(
        "get_prices",
        {"p_sku": code},
    ).execute()

    x = mdates.datestr2num([row["date"] for row in response.data])
    y = [row["price"] for row in response.data]

    response = current_app.supabase_client.rpc(
        "get_item",
        {"p_user_id": usr_id, "p_sku": code},
    ).execute()

    ref = response.data[0]["bid_price"]
    n_day = response.data[0]["frequency"]
    brand = response.data[0]["brand"]
    prod = response.data[0]["name"]

    plt.figure(figsize=(11, 4))
    plt.plot(x, y, label="Price")
    plt.axhline(y=ref, color="r", linestyle="--", label="Target")

    plt.title(
        f"{n_day:.0f} Days Price Trend for {brand}; {prod}",
        loc="left",
        fontproperties=current_app.font,
    )
    plt.title(
        f"As of {datetime.now(current_app.hkt):%Y-%m-%d %H:%M:%S}",
        loc="right",
        fontsize=8,
        style="italic",
    )
    plt.xlabel("Date")
    plt.ylabel("Price (HKD)")
    plt.legend()

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))

    img = BytesIO()
    plt.savefig(img, format="png")
    plt.close()
    img.seek(0)

    return img.getvalue()


def slash_start(usr_id: int, user_name: str, usr_lang: str) -> str:
    response = current_app.supabase_client.rpc(
        "register_user",
        {"p_user_id": usr_id, "p_language": usr_lang},
    ).execute()

    return BotMessages.start(
        user_name,
        response.data[0].get("language"),
        response.data[0].get("status"),
        slash_unk("na"),
    )


def slash_sub(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "change_subscription",
        {"p_user_id": usr_id},
    ).execute()

    return BotMessages.sub(
        response.data[0].get("language"),
        response.data[0].get("is_subscribed"),
        slash_unk("na"),
    )


def slash_unk(status: str) -> str:
    return BotMessages.unk(status)
