#pragma once

#include <string>
#include <vector>

#include "ratelimiter/errors.hpp"
#include "ratelimiter/policy.hpp"

namespace rl {

[[nodiscard]] Result<std::vector<Policy>> load_policies_from_file(const std::string& path);

} // namespace rl
