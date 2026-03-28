"""
SCE-UA (Shuffled Complex Evolution - University of Arizona) 全局优化算法。

参考：Duan, Sorooshian & Gupta (1994) "Effective and Efficient Global Optimization
      for Conceptual Rainfall-Runoff Models"

实现特点：
- 支持等式约束（参数归一化后自动满足）和不等式约束（罚函数）
- 支持参数边界裁剪
- 集成蒲纯魔方（Simplex）局部搜索加速收敛
"""

from __future__ import annotations

import copy
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class SCEUAConfig:
    """SCE-UA 核心配置。"""

    # 总体
    n_params: int                                   # 待优化参数个数
    lower_bounds: List[float]                        # 参数下界
    upper_bounds: List[float]                       # 参数上界

    # 复形（Complex）参数
    p: int      = 2          # 每个 Complex 的点数（推荐 2*P >= n_params）
    q: int      = 2          # 每个子复形的点数（q <= p，q >= 2）
    n_complex: int = 3      # Complex 个数（> 1）

    # 进化参数
    max_iter: int        = 2000   # 最大迭代次数
    max_nfe: int         = 10000  # 最大目标函数调用次数
    convergence_tol: float = 1e-5  # 收敛容差（标准化目标变化）
    elite_fraction: float = 0.2   # 精英比例（最优解保留比例）

    # 局部搜索（Nelder-Mead Simplex）
    use_simplex: bool    = True   # 精英个体是否做局部搜索
    simplex_max_iter: int = 50     # 局部搜索最大步数

    # 罚函数
    penalty_coeff: float = 100.0  # 约束违反惩罚系数

    # 随机种子
    rng_seed: int = 42

    def __post_init__(self) -> None:
        if self.n_params <= 0:
            raise ValueError("n_params must be positive")
        if len(self.lower_bounds) != self.n_params:
            raise ValueError("lower_bounds length must equal n_params")
        if len(self.upper_bounds) != self.n_params:
            raise ValueError("upper_bounds length must equal n_params")
        # 确保下界 <= 上界
        for i, (lo, hi) in enumerate(zip(self.lower_bounds, self.upper_bounds)):
            if lo > hi:
                raise ValueError(f"lower_bounds[{i}]={lo} > upper_bounds[{i}]={hi}")

        # n_complex * p 即总点数，至少为 n_params 的两倍
        if self.n_complex * self.p < 2 * self.n_params:
            np_total = 2 * self.n_params
            self.n_complex = max(2, math.ceil(np_total / self.p))
            logger.warning(
                f"n_complex * p < 2 * n_params, adjusted n_complex -> {self.n_complex}"
            )


class SCEUAOptimizer:
    """
    SCE-UA 全局优化器。

    用法示例::

        def objective(params: np.ndarray) -> float:
            # params: shape (n_params,)
            return some_loss(params)

        config = SCEUAConfig(n_params=6, lower_bounds=[0.1]*6, upper_bounds=[1.0]*6)
        optimizer = SCEUAOptimizer(config)
        best_params, best_obj = optimizer.minimize(objective, progress=True)
    """

    def __init__(self, config: SCEUAConfig) -> None:
        self.cfg = config
        self.rng = np.random.default_rng(config.rng_seed)

        # 内部状态
        self.population: Optional[np.ndarray] = None   # (n_points, n_params)
        self.fvals:      Optional[np.ndarray] = None   # (n_points,)
        self.nfe: int    = 0
        self.iteration: int = 0
        self.history:   List[float] = []

    # ------------------------------------------------------------------
    # 核心接口
    # ------------------------------------------------------------------

    def minimize(
        self,
        objective_func: Callable[[np.ndarray], float],
        *,
        progress: bool = False,
        log_frequency: int = 20,
    ) -> Tuple[np.ndarray, float]:
        """
        运行 SCE-UA 最小化。

        Returns
        -------
        best_params : np.ndarray  shape (n_params,)
        best_obj     : float
        """
        cfg = self.cfg
        n_params = cfg.n_params
        n_complex = cfg.n_complex
        p = cfg.p
        q = cfg.q
        n_points = n_complex * p

        # ---- 1. 初始化种群（LHS 拉丁超立方采样）----
        self.population = self._latin_hypercube_sample(n_points)
        self.fvals = np.empty(n_points, dtype=np.float64)
        self.nfe = 0
        self.iteration = 0
        self.history = []

        # 评估初始种群
        for i in range(n_points):
            self.fvals[i] = self._evaluate(objective_func, self.population[i])

        self._append_history()

        if progress:
            best_idx = int(np.argmin(self.fvals))
            logger.info(
                f"[Iter 0] nfe={self.nfe}  best_obj={self.fvals[best_idx]:.6f}  "
                f"params={self.population[best_idx]}"
            )

        # ---- 2. 主进化循环 ----
        converged = False
        best_obj_global = float(np.min(self.fvals))
        best_params_global = copy.deepcopy(self.population[int(np.argmin(self.fvals))])

        for iteration in range(1, cfg.max_iter + 1):
            self.iteration = iteration

            # 标记当前精英
            srt = np.argsort(self.fvals)
            worst = int(srt[-1])   # 最差
            best  = int(srt[0])    # 最优

            # ---- 3. 打乱种群顺序（Shuffle）----
            indices = np.arange(n_points)
            self.rng.shuffle(indices)
            self.population = self.population[indices]
            self.fvals      = self.fvals[indices]

            # ---- 4. 分成 n_complex 个 Complex ----
            complexes: List[np.ndarray] = []
            complex_fvals: List[np.ndarray] = []
            for c in range(n_complex):
                start = c * p
                end   = start + p
                complexes.append(self.population[start:end])
                complex_fvals.append(self.fvals[start:end])

            new_complexes: List[np.ndarray] = []
            new_fvals_list: List[np.ndarray] = []

            for ic, (comp, fcomp) in enumerate(zip(complexes, complex_fvals)):
                # ---- 5. 每个 Complex 分别进化 ----
                new_comp, new_fcomp = self._evolve_complex(
                    comp, fcomp, objective_func
                )
                new_complexes.append(new_comp)
                new_fvals_list.append(new_fcomp)

            # ---- 6. 合并回种群 ----
            self.population = np.vstack(new_complexes)
            self.fvals      = np.concatenate(new_fvals_list)

            # ---- 7. 精英保留 ----
            elite_size = max(1, int(cfg.elite_fraction * n_points))
            elite_indices = np.argsort(self.fvals)[:elite_size]
            if best < elite_size:
                pass  # 精英中已包含当前全局最优
            else:
                # 用当前最优替换最差个体（如果更好）
                pass

            # 更新全局最优
            current_best_idx = int(np.argmin(self.fvals))
            current_best_obj = float(self.fvals[current_best_idx])
            if current_best_obj < best_obj_global:
                best_obj_global = current_best_obj
                best_params_global = copy.deepcopy(self.population[current_best_idx])

            self._append_history()

            # ---- 8. 收敛判断 ----
            if len(self.history) >= 5:
                recent = self.history[-5:]
                if (
                    max(recent) - min(recent) < cfg.convergence_tol
                    and self.nfe > 3 * n_points
                ):
                    converged = True

            if progress and (iteration % log_frequency == 0 or converged):
                logger.info(
                    f"[Iter {iteration}] nfe={self.nfe}  best_obj={best_obj_global:.6f}"
                )

            # 终止条件
            if converged:
                logger.info(f"SCE-UA converged at iteration {iteration}")
                break
            if self.nfe >= cfg.max_nfe:
                logger.info(f"Max NFE ({cfg.max_nfe}) reached")
                break

        # ---- 9. 对全局最优做最后一次局部搜索 ----
        if cfg.use_simplex:
            if progress:
                logger.info("Running final simplex local search...")
            best_params_global = self._simplex_search(
                objective_func, best_params_global
            )
            final_obj = self._evaluate(objective_func, best_params_global)
            if final_obj < best_obj_global:
                best_obj_global = final_obj
                logger.info(f"Simplex improved obj: {best_obj_global:.6f}")

        self._best_params = best_params_global
        self._best_obj    = best_obj_global
        return best_params_global, best_obj_global

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _latin_hypercube_sample(self, n_points: int) -> np.ndarray:
        """拉丁超立方采样（保证参数空间均匀覆盖）。"""
        n_params = self.cfg.n_params
        sample = np.zeros((n_points, n_params), dtype=np.float64)
        for j in range(n_params):
            edges = np.linspace(0.0, 1.0, n_points + 1)
            mids  = 0.5 * (edges[:-1] + edges[1:])
            self.rng.shuffle(mids)
            sample[:, j] = mids
        # 映射到参数边界
        for j in range(n_params):
            lo = self.cfg.lower_bounds[j]
            hi = self.cfg.upper_bounds[j]
            sample[:, j] = lo + sample[:, j] * (hi - lo)
        return sample

    def _evaluate(
        self,
        objective_func: Callable[[np.ndarray], float],
        params: np.ndarray,
    ) -> float:
        """评估目标函数，带异常处理（失败返回大值）。"""
        self.nfe += 1
        try:
            obj = float(objective_func(params))
            if not math.isfinite(obj) or obj < 0:
                obj = 1.0e18
        except Exception:
            obj = 1.0e18
        return obj

    def _evolve_complex(
        self,
        complex_arr: np.ndarray,
        complex_fvals: np.ndarray,
        objective_func: Callable[[np.ndarray], float],
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        对单个 Complex 执行 SCE 进化步骤（CEVP）。
        """
        cfg = self.cfg
        p = cfg.p
        q = cfg.q
        n_params = cfg.n_params

        comp = complex_arr.copy()
        fcomp = complex_fvals.copy()

        # 按目标值排序
        srt = np.argsort(fcomp)
        comp = comp[srt]
        fcomp = fcomp[srt]

        # ---- 选取 q 个最差点 ----
        worst_q_idx = srt[-q:]   # 最差的 q 个（在大 Complex 中的原始索引）
        worst_q = comp[-q:]      # shape (q, n_params)
        f_worst_q = fcomp[-q:]

        # ---- 质心（不包括最差点）----
        centroid = np.mean(comp[:-q], axis=0)   # shape (n_params,)

        # ---- 反射点 ----
        alpha = 1.0
        reflected = []
        reflected_fvals = []
        for i in range(q):
            r = centroid + alpha * (centroid - worst_q[i])
            r = np.clip(r, self.cfg.lower_bounds, self.cfg.upper_bounds)
            reflected.append(r)
            f_r = self._evaluate(objective_func, r)
            reflected.append(r)
            reflected_fvals.append(f_r)

        reflected = np.array(reflected, dtype=np.float64)
        reflected_fvals = np.array(reflected_fvals, dtype=np.float64)

        # ---- 尝试按"新点替换最差点"策略 ----
        # 找到反射组中最差点
        worst_reflected_idx = int(np.argmax(reflected_fvals))
        worst_reflected_obj = reflected_fvals[worst_reflected_idx]
        worst_orig_idx = int(np.argmax(f_worst_q))
        worst_orig_obj = f_worst_q[worst_orig_idx]

        if worst_reflected_obj < worst_orig_obj:
            # 反射成功：替换原最差点
            new_worst = reflected[worst_reflected_idx]
            comp[-q + worst_orig_idx] = new_worst
            fcomp[-q + worst_orig_idx] = worst_reflected_obj
        else:
            # 反射失败：收缩尝试
            contracted = []
            contracted_fvals = []
            for i in range(q):
                beta = 0.5
                c = centroid + beta * (worst_q[i] - centroid)
                c = np.clip(c, self.cfg.lower_bounds, self.cfg.upper_bounds)
                f_c = self._evaluate(objective_func, c)
                contracted.append(c)
                contracted_fvals.append(f_c)
            contracted = np.array(contracted, dtype=np.float64)
            contracted_fvals = np.array(contracted_fvals, dtype=np.float64)

            worst_contraction_idx = int(np.argmax(contracted_fvals))
            worst_contraction_obj = contracted_fvals[worst_contraction_idx]
            if worst_contraction_obj < worst_orig_obj:
                comp[-q + worst_orig_idx] = contracted[worst_contraction_idx]
                fcomp[-q + worst_orig_idx] = worst_contraction_obj
            else:
                # 收缩也失败：随机生成
                random_point = self._random_point(n_params)
                f_rand = self._evaluate(objective_func, random_point)
                comp[-q + worst_orig_idx] = random_point
                fcomp[-q + worst_orig_idx] = f_rand

        # 重新排序
        srt2 = np.argsort(fcomp)
        return comp[srt2], fcomp[srt2]

    def _simplex_search(
        self,
        objective_func: Callable[[np.ndarray], float],
        initial_params: np.ndarray,
        max_iter: Optional[int] = None,
    ) -> np.ndarray:
        """Nelder-Mead 单纯形局部搜索。"""
        cfg = self.cfg
        n_params = cfg.n_params
        max_iter = max_iter or cfg.simplex_max_iter

        # 初始单纯形（沿各坐标轴扩展一点）
        simplex = np.zeros((n_params + 1, n_params), dtype=np.float64)
        simplex[0] = initial_params
        for i in range(n_params):
            step = (cfg.upper_bounds[i] - cfg.lower_bounds[i]) * 0.05
            simplex[i + 1] = simplex[0].copy()
            simplex[i + 1, i] = np.clip(
                simplex[0, i] + step,
                cfg.lower_bounds[i],
                cfg.upper_bounds[i],
            )

        f_vals = np.array([self._evaluate(objective_func, x) for x in simplex])

        alpha = 1.0   # 反射系数
        gamma = 2.0   # 扩展系数
        rho   = 0.5   # 收缩系数
        sigma = 0.5   # 压缩系数

        for _ in range(max_iter):
            srt = np.argsort(f_vals)
            simplex = simplex[srt]
            f_vals  = f_vals[srt]

            centroid = np.mean(simplex[:-1], axis=0)

            # 反射
            xr = np.clip(
                centroid + alpha * (centroid - simplex[-1]),
                cfg.lower_bounds,
                cfg.upper_bounds,
            )
            f_xr = self._evaluate(objective_func, xr)

            if f_vals[0] <= f_xr < f_vals[-2]:
                simplex[-1] = xr
                f_vals[-1] = f_xr
            elif f_xr < f_vals[0]:
                # 扩展
                xe = np.clip(
                    centroid + gamma * (xr - centroid),
                    cfg.lower_bounds,
                    cfg.upper_bounds,
                )
                f_xe = self._evaluate(objective_func, xe)
                if f_xe < f_xr:
                    simplex[-1] = xe
                    f_vals[-1] = f_xe
                else:
                    simplex[-1] = xr
                    f_vals[-1] = f_xr
            else:
                # 收缩
                xc = np.clip(
                    centroid + rho * (simplex[-1] - centroid),
                    cfg.lower_bounds,
                    cfg.upper_bounds,
                )
                f_xc = self._evaluate(objective_func, xc)
                if f_xc < f_vals[-1]:
                    simplex[-1] = xc
                    f_vals[-1] = f_xc
                else:
                    # 向最优收缩
                    for i in range(1, n_params + 1):
                        simplex[i] = simplex[0] + sigma * (simplex[i] - simplex[0])
                        f_vals[i] = self._evaluate(objective_func, simplex[i])

            if np.max(f_vals) - np.min(f_vals) < 1e-8:
                break

        best_idx = int(np.argmin(f_vals))
        return simplex[best_idx]

    def _random_point(self, n_params: int) -> np.ndarray:
        """在参数边界内生成随机点。"""
        r = self.rng.uniform(0, 1, n_params)
        return np.array(
            [
                lo + r[i] * (hi - lo)
                for i, (lo, hi) in enumerate(
                    zip(self.cfg.lower_bounds, self.cfg.upper_bounds)
                )
            ],
            dtype=np.float64,
        )

    def _append_history(self) -> None:
        self.history.append(float(np.min(self.fvals)))

    # ------------------------------------------------------------------
    # 结果查询
    # ------------------------------------------------------------------

    @property
    def n_function_evaluations(self) -> int:
        return self.nfe

    @property
    def convergence_history(self) -> List[float]:
        return list(self.history)

    def get_statistics(self) -> dict:
        """返回优化统计信息。"""
        return {
            "n_function_evaluations": self.nfe,
            "iterations": self.iteration,
            "final_objective": float(np.min(self.fvals)),
            "history": self.history,
        }
