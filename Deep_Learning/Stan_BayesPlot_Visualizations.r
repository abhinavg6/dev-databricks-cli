# Databricks notebook source
library("bayesplot")
library("rstanarm")
library("ggplot2")

fit <- stan_glm(mpg ~ ., data = mtcars)
posterior <- as.matrix(fit)

plot_title <- ggtitle("Posterior distributions",
                      "with medians and 80% intervals")
mcmc_areas(posterior, 
           pars = c("cyl", "drat", "am", "wt"), 
           prob = 0.8) + plot_title

# COMMAND ----------

color_scheme_set("red")
ppc_dens_overlay(y = fit$y, 
                 yrep = posterior_predict(fit, draws = 50))

# COMMAND ----------

# also works nicely with piping
library("dplyr")
color_scheme_set("brightblue")
fit %>% 
  posterior_predict(draws = 500) %>%
  ppc_stat_grouped(y = mtcars$mpg, 
                   group = mtcars$carb, 
                   stat = "median")

# COMMAND ----------

# with rstan demo model
library("rstan")
fit2 <- stan_demo("eight_schools", warmup = 300, iter = 700)
posterior2 <- extract(fit2, inc_warmup = TRUE, permuted = FALSE)

color_scheme_set("mix-blue-pink")
p <- mcmc_trace(posterior2,  pars = c("mu", "tau"), n_warmup = 300,
                facet_args = list(nrow = 2, labeller = label_parsed))
p + facet_text(size = 15)

# COMMAND ----------

# scatter plot also showing divergences
color_scheme_set("darkgray")
mcmc_scatter(
  as.matrix(fit2),
  pars = c("tau", "theta[1]"), 
  np = nuts_params(fit2), 
  np_style = scatter_style_np(div_color = "green", div_alpha = 0.8)
)

# COMMAND ----------

color_scheme_set("red")
np <- nuts_params(fit2)
mcmc_nuts_energy(np) + ggtitle("NUTS Energy Diagnostic")

# COMMAND ----------

# another example with rstanarm
color_scheme_set("purple")

fit <- stan_glmer(mpg ~ wt + (1|cyl), data = mtcars)
ppc_intervals(
  y = mtcars$mpg,
  yrep = posterior_predict(fit),
  x = mtcars$wt,
  prob = 0.5
) +
  labs(
    x = "Weight (1000 lbs)",
    y = "MPG",
    title = "50% posterior predictive intervals \nvs observed miles per gallon",
    subtitle = "by vehicle weight"
  ) +
  panel_bg(fill = "gray95", color = NA) +
  grid_lines(color = "white")