# Databricks notebook source
# MAGIC %md Replicating Linear Mixed Effects Models Notebook from [here](http://nbviewer.jupyter.org/github/blei-lab/edward/blob/master/notebooks/linear_mixed_effects_models.ipynb)

# COMMAND ----------

import edward as ed
import pandas as pd
import tensorflow as tf
import matplotlib.pyplot as plt

# COMMAND ----------

from edward.models import Normal
from observations import insteval

plt.style.use('ggplot')
ed.set_seed(42)

# COMMAND ----------

data, metadata = insteval("~/data")
data = pd.DataFrame(data, columns=metadata['columns'])

# s - students - 1:2972
# d - instructors - codes that need to be remapped
# dept also needs to be remapped
data['s'] = data['s'] - 1
data['dcodes'] = data['d'].astype('category').cat.codes
data['deptcodes'] = data['dept'].astype('category').cat.codes
data['y'] = data['y'].values.astype(float)

train = data.sample(frac=0.8)
test = data.drop(train.index)

train.head()

# COMMAND ----------

s_train = train['s'].values
d_train = train['dcodes'].values
dept_train = train['deptcodes'].values
y_train = train['y'].values
service_train = train['service'].values
n_obs_train = train.shape[0]

s_test = test['s'].values
d_test = test['dcodes'].values
dept_test = test['deptcodes'].values
y_test = test['y'].values
service_test = test['service'].values
n_obs_test = test.shape[0]

# COMMAND ----------

n_s = max(s_train) + 1  # number of students
n_d = max(d_train) + 1  # number of instructors
n_dept = max(dept_train) + 1  # number of departments
n_obs = train.shape[0]  # number of observations

print("Number of students: {}".format(n_s))
print("Number of instructors: {}".format(n_d))
print("Number of departments: {}".format(n_dept))
print("Number of observations: {}".format(n_obs))

# COMMAND ----------

# Set up placeholders for the data inputs.
s_ph = tf.placeholder(tf.int32, [None])
d_ph = tf.placeholder(tf.int32, [None])
dept_ph = tf.placeholder(tf.int32, [None])
service_ph = tf.placeholder(tf.float32, [None])

# Set up fixed effects.
mu = tf.Variable(tf.random_normal([]))
service = tf.Variable(tf.random_normal([]))

sigma_s = tf.sqrt(tf.exp(tf.Variable(tf.random_normal([]))))
sigma_d = tf.sqrt(tf.exp(tf.Variable(tf.random_normal([]))))
sigma_dept = tf.sqrt(tf.exp(tf.Variable(tf.random_normal([]))))

# Set up random effects.
eta_s = Normal(loc=tf.zeros(n_s), scale=sigma_s * tf.ones(n_s))
eta_d = Normal(loc=tf.zeros(n_d), scale=sigma_d * tf.ones(n_d))
eta_dept = Normal(loc=tf.zeros(n_dept), scale=sigma_dept * tf.ones(n_dept))

yhat = (tf.gather(eta_s, s_ph) +
        tf.gather(eta_d, d_ph) +
        tf.gather(eta_dept, dept_ph) +
        mu + service * service_ph)
y = Normal(loc=yhat, scale=tf.ones(n_obs))

# COMMAND ----------

q_eta_s = Normal(
    loc=tf.Variable(tf.random_normal([n_s])),
    scale=tf.nn.softplus(tf.Variable(tf.random_normal([n_s]))))
q_eta_d = Normal(
    loc=tf.Variable(tf.random_normal([n_d])),
    scale=tf.nn.softplus(tf.Variable(tf.random_normal([n_d]))))
q_eta_dept = Normal(
    loc=tf.Variable(tf.random_normal([n_dept])),
    scale=tf.nn.softplus(tf.Variable(tf.random_normal([n_dept]))))

latent_vars = {
    eta_s: q_eta_s,
    eta_d: q_eta_d,
    eta_dept: q_eta_dept}
data = {
    y: y_train,
    s_ph: s_train,
    d_ph: d_train,
    dept_ph: dept_train,
    service_ph: service_train}
inference = ed.KLqp(latent_vars, data)

# COMMAND ----------

yhat_test = ed.copy(yhat, {
    eta_s: q_eta_s.mean(),
    eta_d: q_eta_d.mean(),
    eta_dept: q_eta_dept.mean()})

# COMMAND ----------

inference.initialize(n_print=2000, n_iter=10000)
tf.global_variables_initializer().run()

for _ in range(inference.n_iter):
  # Update and print progress of algorithm.
  info_dict = inference.update()
  inference.print_progress(info_dict)

  t = info_dict['t']
  if t == 1 or t % inference.n_print == 0:
    # Make predictions on test data.
    yhat_vals = yhat_test.eval(feed_dict={
        s_ph: s_test,
        d_ph: d_test,
        dept_ph: dept_test,
        service_ph: service_test})
    # Removed plot display from here, as we don't support incremental displays within a loop using display() function

# COMMAND ----------

# MAGIC %md Upload the below files from [here](http://nbviewer.jupyter.org/github/blei-lab/edward/tree/master/notebooks/data/) using [Data UI](https://docs.databricks.com/user-guide/tables.html#creating-tables)

# COMMAND ----------

student_effects_lme4 = pd.read_csv('/dbfs/FileStore/tables/insteval_student_ranefs_r.csv')
instructor_effects_lme4 = pd.read_csv('/dbfs/FileStore/tables/insteval_instructor_ranefs_r.csv')
dept_effects_lme4 = pd.read_csv('/dbfs/FileStore/tables/insteval_dept_ranefs_r.csv')

# COMMAND ----------

student_effects_edward = q_eta_s.mean().eval()
instructor_effects_edward = q_eta_d.mean().eval()
dept_effects_edward = q_eta_dept.mean().eval()

# COMMAND ----------

plt.title("Student Effects Comparison")
plt.xlim(-1, 1)
plt.ylim(-1, 1)
plt.xlabel("Student Effects from lme4")
plt.ylabel("Student Effects from edward")
plt.scatter(student_effects_lme4["(Intercept)"],
            student_effects_edward,
            alpha=0.25)
display(plt.show())

# COMMAND ----------

plt.title("Instructor Effects Comparison")
plt.xlim(-1.5, 1.5)
plt.ylim(-1.5, 1.5)
plt.xlabel("Instructor Effects from lme4")
plt.ylabel("Instructor Effects from edward")
plt.scatter(instructor_effects_lme4["(Intercept)"],
            instructor_effects_edward,
            alpha=0.25)
display(plt.show())

# COMMAND ----------

#  Add in the intercept from R and edward
dept_effects_and_intercept_lme4 = 3.28259 + dept_effects_lme4["(Intercept)"]
dept_effects_and_intercept_edward = mu.eval() + dept_effects_edward

# COMMAND ----------

plt.title("Departmental Effects Comparison")
plt.xlim(3.0, 3.5)
plt.ylim(3.0, 3.5)
plt.xlabel("Department Effects from lme4")
plt.ylabel("Department Effects from edward")
plt.scatter(dept_effects_and_intercept_lme4,
            dept_effects_and_intercept_edward,
            s=0.01 * train.dept.value_counts())
display(plt.show())