import re

import luigi
import polars as pl

from .cleanse import OpwCleanser
from ..models import PromoCategory
from ...shared.io import read_target, write_target
from ...shared.logging import LUIGI_LOGGER
from ...shared.settings import BASE_DIR, Settings


class OpwAnalyser(luigi.Task):
    """Categorise promotions and calculate unit prices."""
    date = luigi.DateParameter()

    def requires(self):
        return OpwCleanser(date=self.date)

    def output(self):
        return luigi.LocalTarget(
            BASE_DIR
            / "data"
            / "processed"
            / f"prices_{self.date:%Y%m%d}.parquet"
        )

    def run(self) -> None:
        df_price = read_target(self.input()[1])
        df_price, cnt = self._calculate_promotion_prices(df_price)

        write_target(df_price, self.output())

        LUIGI_LOGGER.info(f"\t- Total of discount prices: {cnt:,}")

    def _split_components(self, mkt_txt: str) -> list[str]:
        txt = re.sub(r"\s?wk\d+\s?", "", mkt_txt.lower().strip())
        txt = re.sub(r"; |/|[a-z]\.[a-z]", "<sep>", txt)
        txt = re.sub(r"half price", "50%", txt)
        txt = re.sub(r"second", "2nd", txt)

        return list(map(str.strip, txt.split("<sep>")))

    def _map_pattern_tags(self, promotion: str) -> str:
        pattern_tags = {
            r"\$\d+(\.\d+)?": "<AMT>",
            r"\d+(\.\d+)?\%": "<PCT>",
            r"\d+": "<CNT>",
        }

        txt = promotion.lower().strip()
        for pat, tag in pattern_tags.items():
            txt = re.sub(pat, tag, txt)

        return txt

    def _extract_tag_values(self, data: dict[str, str]) -> list[float]:
        values = []
        if data["category"]:
            promotions = data["promotion"].split()
            patterns = data["pattern"].split()
            for promotion, pattern in zip(promotions, patterns):
                if re.search(r"<AMT>|<CNT>|<PCT>", pattern):
                    nums = re.findall(r"(\d+\.?\d{,2})", promotion)
                    values += [float(num) for num in nums]

        return values

    def _calculate_discount_price(
        self,
        data: dict[str, float | list[float]],
    ) -> float:
        price, pattern, category, values = data.values()
        discount = price

        try:
            match category:
                case PromoCategory.AMT_FOR_CNT:
                    if pattern == "+<AMT> for <CNT>nd item":
                        discount = price + values[0]
                        discount /= values[1]
                    elif pattern == "<AMT> for <CNT>":
                        discount = values[0] / values[1]
                case PromoCategory.BUY_N_SAVE_AMT:
                    if re.search(r"<CNT>\s.*save", pattern):
                        discount = price * values[0] - values[1]
                        discount /= values[0]
                    elif re.search(r"<CNT>\s", pattern):
                        discount = values[1] / values[0]
                case PromoCategory.BUY_N_GET_N_FREE:
                    if re.search(r"free the most expensive one", pattern):
                        discount = price * (values[0] - 1)
                        discount /= values[0]
                    elif re.search(r"get <CNT> free", pattern):
                        discount = price * values[0]
                        discount /= values[0] + values[1]
                case PromoCategory.BUY_N_PCT_OFF:
                    if re.search(r"<CNT>\w", pattern):
                        discount = price * (values[0] - 1)
                        discount += price * (1 - values[1] / 100)
                        discount /= values[0]
                    else:
                        discount = price * values[0] * (1 - values[1] / 100)
                        discount /= values[0]
                case PromoCategory.PCT_FOR_N:
                    if re.search(r"<CNT>\w", pattern):
                        discount = price * (1 - values[0] / 100)
                        discount += price * (values[1] - 1)
                        discount /= values[1]
                    else:
                        discount = price * (1 - values[0] / 100)
                case _:
                    discount = price
        except Exception:  # ignore promotions that do not comply to rules
            pass

        return discount if price * Settings.THRESHOLD < discount else price

    def _calculate_promotion_prices(
        self,
        df_price: pl.DataFrame,
    ) -> pl.DataFrame:
        if df_price.is_empty():
            return df_price, 0

        df_price = (
            df_price
            .with_columns(
                pl.col("promotion_en")
                .map_elements(
                    self._split_components,
                    return_dtype=list[str],
                )
                .alias("promotion"),  # split description with multiple promotions
            )
            .explode("promotion")  # expand row per promotion
            .with_columns(
                pl.col("promotion")
                .map_elements(self._map_pattern_tags, return_dtype=str)
                .alias("pattern"),  # get <AMT>, <CNT> and <PCT> as pattern
            )
            .with_columns(
                pl.col("pattern").str.count_matches("<AMT>").alias("amt"),
                pl.col("pattern").str.count_matches("<CNT>").alias("cnt"),
                pl.col("pattern").str.count_matches("<PCT>").alias("pct"),
            )
            .with_columns(
                pl.when(  # NA
                    pl.col("pattern").str.contains(r"<AMT>.*<AMT>")
                    & (pl.col("amt") == 2)
                    & (pl.col("cnt") == 0)
                    & (pl.col("pct") == 0)
                ).then(PromoCategory.AMT_AMT)
                .when(  # <$> for <n>
                    pl.col("pattern").str.contains(r"<AMT>.*<CNT>")
                    & (pl.col("amt") == 1)
                    & (pl.col("cnt") == 1)
                    & (pl.col("pct") == 0)
                ).then(PromoCategory.AMT_FOR_CNT)
                .when(  # NA
                    pl.col("pattern").str.contains(r"<AMT>.*<PCT>")
                    & (pl.col("amt") == 1)
                    & (pl.col("cnt") == 0)
                    & (pl.col("pct") == 1)
                ).then(PromoCategory.AMT_PCT)
                .when(  # buy <n> at/save <$>
                    pl.col("pattern").str.contains(r"<CNT>.*<AMT>")
                    & (pl.col("amt") == 1)
                    & (pl.col("cnt") == 1)
                    & (pl.col("pct") == 0)
                ).then(PromoCategory.BUY_N_SAVE_AMT)
                .when(  # buy <n> get <n> free
                    pl.col("pattern").str.contains(r"<CNT>.*<CNT>")
                    & (pl.col("amt") == 0)
                    & (pl.col("cnt") == 2)
                    & (pl.col("pct") == 0)
                ).then(PromoCategory.BUY_N_GET_N_FREE)
                .when(  # buy <n> at <%>
                    pl.col("pattern").str.contains(r"<CNT>.*<PCT>")
                    & (pl.col("amt") == 0)
                    & (pl.col("cnt") == 1)
                    & (pl.col("pct") == 1)
                ).then(PromoCategory.BUY_N_PCT_OFF)
                .when(  # NA
                    pl.col("pattern").str.contains(r"<PCT>.*<AMT>")
                    & (pl.col("amt") == 1)
                    & (pl.col("cnt") == 0)
                    & (pl.col("pct") == 1)
                ).then(PromoCategory.PCT_AMT)
                .when(  # <%> for <n>
                    pl.col("pattern").str.contains(r"<PCT>.*<CNT>")
                    & (pl.col("amt") == 0)
                    & (pl.col("cnt") == 1)
                    & (pl.col("pct") == 1)
                ).then(PromoCategory.PCT_FOR_N)
                .when(  # NA
                    pl.col("pattern").str.contains(r"<PCT>.*<PCT>")
                    & (pl.col("amt") == 0)
                    & (pl.col("cnt") == 0)
                    & (pl.col("pct") == 2)
                ).then(PromoCategory.PCT_PCT)
                .otherwise(0)
                .alias("category"),  # categorise promotion into 9 groups
            )
            .with_columns(
                pl.struct("promotion", "pattern", "category")
                .map_elements(
                    self._extract_tag_values,
                    return_dtype=list[float],
                )
                .alias("value"),  # get numeric values according to pattern
            )
            .with_columns(
                pl.struct("original_price", "pattern", "category", "value")
                .map_elements(
                    self._calculate_discount_price,
                    return_dtype=float,
                )
                .alias("unit_price"),  # calculate price based on defined rules
            )
            .sort(["sku", "effective_date", "supermarket", "unit_price"])
            .unique(subset=["sku", "effective_date", "supermarket"])
            .drop([
                "amt", "cnt", "pct",
                "promotion", "pattern", "category", "value",
            ])
        )

        cnt = (
            df_price
            .filter(pl.col("original_price") != pl.col("unit_price"))
            .shape[0]
        )

        return df_price, cnt
