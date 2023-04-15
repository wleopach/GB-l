from umap import validation
from warnings import filterwarnings
from utils import load_model
import matplotlib as plt
clf = load_model('model.joblib')
K = 3
# supress numba deprecation warnings from UMAP
filterwarnings('ignore')
numerical_trustworthiness = validation.trustworthiness_vector(source=clf.numerical_umap_._raw_data
                                                              , embedding=clf.numerical_umap_.embedding_
                                                              , max_k=K)
categorical_trustworthiness = validation.trustworthiness_vector(source=clf.categorical_umap_._raw_data
                                                                , embedding=clf.categorical_umap_.embedding_
                                                                , max_k=K)
filterwarnings('default')
# plt.plot(categorical_trustworthiness)
plt.plot(numerical_trustworthiness)
plt.ylabel("Trustworthiness score")
plt.xlabel("Value of K")
plt.title(f"Trustworthiness at {K}")
plt.ylim(0,1)
plt.legend(["numerical T", "categorical T"], loc="upper right")
plt.show()

