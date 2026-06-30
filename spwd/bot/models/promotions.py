from enum import Enum


class PromotionDays(Enum):
    WELLCOME = ((3, 13, 23), "Wellcome enJoy Card 8% off")
    PARKNSHOP = ((2, 12, 22), "ParknShop Platinum Card 8% off")

    @property
    def promo_dates(self):
        return self.value[0]

    @property
    def offer(self):
        return self.value[1]

    def get_card_offer(self, dates) -> str:
        day = next(
            (i for i, date in enumerate(dates) if date in self.promo_dates),
            None,
        )

        if day is None:
            return ""
        if day == 0:
            return f"\n💳 {self.offer}: TODAY."

        return f"\n💳 {self.offer}: {day} more day(s)."
