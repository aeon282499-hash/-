"""
競馬予測モデル (LightGBM ベース)

アーキテクチャ:
  - LightGBM 二値分類 (1着かどうか)
  - 確率キャリブレーション (IsotonicRegression)
  - Time-Series Cross Validation (過去データで学習 → 未来データで評価)
  - 特徴量重要度の記録
"""

import logging
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import lightgbm as lgb
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.isotonic import IsotonicRegression
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import log_loss, roc_auc_score
    LGBM_AVAILABLE = True
except ImportError:
    LGBM_AVAILABLE = False
    logger.warning("lightgbm/sklearn not installed. Run: pip install lightgbm scikit-learn")


class KeibaPredictor:
    """
    競馬レース勝利確率予測モデル。

    使い方:
        model = KeibaPredictor()
        model.train(X_train, y_train, X_val, y_val)
        proba = model.predict_proba(X_test)
        model.save("models/keiba_model.pkl")
    """

    def __init__(self, params: dict | None = None):
        if not LGBM_AVAILABLE:
            raise ImportError("lightgbm and scikit-learn are required.")
        from keiba_ai.config import LIGHTGBM_PARAMS
        self.params = params or LIGHTGBM_PARAMS.copy()
        self.model: Optional[lgb.LGBMClassifier] = None
        self.calibrator: Optional[IsotonicRegression] = None
        self.feature_names: list[str] = []
        self.feature_importances_: Optional[pd.Series] = None
        self._trained = False

    # ------------------------------------------------------------------
    # 学習
    # ------------------------------------------------------------------

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame | None = None,
        y_val: pd.Series | None = None,
        early_stopping_rounds: int = 50,
    ) -> "KeibaPredictor":
        """
        LightGBM を学習し、Isotonic Regression でキャリブレーションする。
        """
        self.feature_names = list(X_train.columns)

        # LightGBM
        self.model = lgb.LGBMClassifier(**self.params)

        fit_kwargs: dict = {}
        if X_val is not None and y_val is not None:
            fit_kwargs["eval_set"] = [(X_val, y_val)]
            fit_kwargs["callbacks"] = [
                lgb.early_stopping(early_stopping_rounds, verbose=False),
                lgb.log_evaluation(period=100),
            ]

        self.model.fit(X_train, y_train, **fit_kwargs)
        logger.info(f"Best iteration: {self.model.best_iteration_}")

        # 確率キャリブレーション
        if X_val is not None and y_val is not None:
            raw_proba = self.model.predict_proba(X_val)[:, 1]
            self.calibrator = IsotonicRegression(out_of_bounds="clip")
            self.calibrator.fit(raw_proba, y_val)
            cal_proba = self.calibrator.predict(raw_proba)
            logger.info(
                f"Calibration: log_loss {log_loss(y_val, raw_proba):.4f} → "
                f"{log_loss(y_val, cal_proba):.4f} | "
                f"AUC {roc_auc_score(y_val, raw_proba):.4f}"
            )

        # 特徴量重要度
        self.feature_importances_ = pd.Series(
            self.model.feature_importances_,
            index=self.feature_names,
        ).sort_values(ascending=False)

        self._trained = True
        logger.info("Training complete.")
        return self

    def train_cv(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        n_splits: int = 5,
    ) -> dict:
        """
        時系列クロスバリデーション。
        各 fold の評価指標を返す。
        """
        tscv = TimeSeriesSplit(n_splits=n_splits)
        scores = {"log_loss": [], "auc": []}

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            X_tr, X_vl = X.iloc[train_idx], X.iloc[val_idx]
            y_tr, y_vl = y.iloc[train_idx], y.iloc[val_idx]

            model = lgb.LGBMClassifier(**self.params)
            model.fit(
                X_tr, y_tr,
                eval_set=[(X_vl, y_vl)],
                callbacks=[
                    lgb.early_stopping(50, verbose=False),
                    lgb.log_evaluation(period=-1),
                ],
            )

            proba = model.predict_proba(X_vl)[:, 1]
            ll = log_loss(y_vl, proba)
            auc = roc_auc_score(y_vl, proba) if y_vl.sum() > 0 else 0.5
            scores["log_loss"].append(ll)
            scores["auc"].append(auc)
            logger.info(f"Fold {fold+1}: log_loss={ll:.4f} auc={auc:.4f}")

        logger.info(
            f"CV Results: log_loss={np.mean(scores['log_loss']):.4f}±{np.std(scores['log_loss']):.4f} "
            f"auc={np.mean(scores['auc']):.4f}±{np.std(scores['auc']):.4f}"
        )

        # 最終モデルは全データで学習
        self.train(X, y)
        return scores

    # ------------------------------------------------------------------
    # 予測
    # ------------------------------------------------------------------

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        各馬の1着確率を返す (キャリブレーション済み)。
        """
        if not self._trained or self.model is None:
            raise RuntimeError("Model not trained. Call train() first.")

        X_aligned = X.reindex(columns=self.feature_names, fill_value=0)
        raw = self.model.predict_proba(X_aligned)[:, 1]

        if self.calibrator is not None:
            return self.calibrator.predict(raw)
        return raw

    def predict_race(self, race_X: pd.DataFrame, horse_nos: list) -> pd.DataFrame:
        """
        1レースの全馬に対して予測し、ソート済み DataFrame を返す。

        Returns:
            DataFrame with columns: horse_no, pred_prob, pred_rank
        """
        proba = self.predict_proba(race_X)
        result = pd.DataFrame({
            "horse_no": horse_nos,
            "pred_prob": proba,
        })
        result["pred_rank"] = result["pred_prob"].rank(ascending=False, method="first").astype(int)
        return result.sort_values("pred_prob", ascending=False).reset_index(drop=True)

    # ------------------------------------------------------------------
    # 保存・読み込み
    # ------------------------------------------------------------------

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)
        logger.info(f"Model saved → {path}")

    @classmethod
    def load(cls, path: str | Path) -> "KeibaPredictor":
        with open(path, "rb") as f:
            obj = pickle.load(f)
        logger.info(f"Model loaded ← {path}")
        return obj

    # ------------------------------------------------------------------
    # 診断
    # ------------------------------------------------------------------

    def print_feature_importance(self, top_n: int = 20) -> None:
        if self.feature_importances_ is None:
            print("Not trained yet.")
            return
        print(f"\n=== Top {top_n} Feature Importances ===")
        for feat, imp in self.feature_importances_.head(top_n).items():
            bar = "█" * int(imp / self.feature_importances_.max() * 30)
            print(f"  {feat:<35} {bar} ({imp:.0f})")


# ---------------------------------------------------------------------------
# スタンドアロン学習スクリプト
# ---------------------------------------------------------------------------

def train_from_csv(
    csv_path: str,
    model_output: str = "keiba_ai/models/model.pkl",
    history_dir: str | None = None,
) -> KeibaPredictor:
    """
    CSV ファイルから学習する。

    Args:
        csv_path: scraper.collect_race_results() で生成した CSV
        model_output: モデル保存先
        history_dir: 馬の過去成績 CSV が入ったディレクトリ (任意)
    """
    from keiba_ai.features import build_training_dataset

    logger.info(f"Loading data from {csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    # 過去成績キャッシュの読み込み
    history_dict: dict = {}
    if history_dir:
        hist_path = Path(history_dir)
        for f in hist_path.glob("*.csv"):
            horse_id = f.stem
            history_dict[horse_id] = pd.read_csv(f)
        logger.info(f"Loaded {len(history_dict)} horse histories")

    # 時系列分割 (前80%を学習、後20%を検証)
    df = df.sort_values("race_date") if "race_date" in df.columns else df
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    val_df = df.iloc[split_idx:]

    X_train, y_train = build_training_dataset(train_df, history_dict)
    X_val, y_val = build_training_dataset(val_df, history_dict)

    model = KeibaPredictor()
    model.train(X_train, y_train, X_val, y_val)
    model.print_feature_importance()
    model.save(model_output)

    return model
