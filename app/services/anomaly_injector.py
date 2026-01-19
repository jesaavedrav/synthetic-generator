from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional

class BaseAnomalyInjector(ABC):
    def __init__(self, columns: Optional[List[str]] = None, params: Optional[Dict[str, Any]] = None):
        self.columns = columns
        self.params = params or {}

    @abstractmethod
    def inject(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class OutlierInjector(BaseAnomalyInjector):
    def inject(self, df: pd.DataFrame) -> pd.DataFrame:
        factor = self.params.get("factor", 3)
        columns = self.columns or df.select_dtypes(include=[np.number]).columns.tolist()
        df_out = df.copy()
        for col in columns:
            if col in df_out:
                n = int(len(df_out) * self.params.get("proportion", 0.01))
                idx = np.random.choice(df_out.index, n, replace=False)
                std = df_out[col].std()
                mean = df_out[col].mean()
                df_out.loc[idx, col] = mean + factor * std
        return df_out

class MissingValueInjector(BaseAnomalyInjector):
    def inject(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = self.columns or df.columns.tolist()
        df_out = df.copy()
        for col in columns:
            if col in df_out:
                n = int(len(df_out) * self.params.get("proportion", 0.01))
                idx = np.random.choice(df_out.index, n, replace=False)
                df_out.loc[idx, col] = np.nan
        return df_out

class CategoryNoiseInjector(BaseAnomalyInjector):
    def inject(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = self.columns or df.select_dtypes(include=[object]).columns.tolist()
        df_out = df.copy()
        for col in columns:
            if col in df_out:
                n = int(len(df_out) * self.params.get("proportion", 0.01))
                idx = np.random.choice(df_out.index, n, replace=False)
                categories = df_out[col].dropna().unique()
                for i in idx:
                    df_out.at[i, col] = np.random.choice(categories)
        return df_out

class AnomalyInjectorFactory:
    _injectors = {
        "outlier": OutlierInjector,
        "missing": MissingValueInjector,
        "category_noise": CategoryNoiseInjector,
    }

    @classmethod
    def get_injector(cls, anomaly_type: str, columns: Optional[List[str]] = None, params: Optional[Dict[str, Any]] = None) -> BaseAnomalyInjector:
        if anomaly_type not in cls._injectors:
            raise ValueError(f"Unknown anomaly type: {anomaly_type}")
        return cls._injectors[anomaly_type](columns, params)
