# =====================================================================
# framework/dv_bootstrap.py
# One call to make ALL Gold/ADV rules available on the base RuleEngine:
#   - DV rules (record_source, business_key, dv_audit, hardcode,
#     filter_not_null, filter_expr)  via DVRuleEngine.install()
#   - the new `derive` rule                via install_derive_rule()
#
# Call once at the top of the gold notebook, right after imports:
#       from framework.dv_bootstrap import install_gold_rules
#       install_gold_rules()
# =====================================================================
import logging
from framework.dv_rule_engine import DVRuleEngine
from framework.dv_derive_rule import install_derive_rule

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)

def install_gold_rules():
    injected = DVRuleEngine.install()
    injected += install_derive_rule()
    logger.info("[dv_bootstrap] Gold rules installed: %s", injected)
    return injected
