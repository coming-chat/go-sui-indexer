package cache

const (
	pushNewTxDigestToStream = `
		local lastDigest = redis.call('get', KEYS[2])
		local result = false
		if ((lastDigest ~= false) and (lastDigest == ARGV[2])) or ((lastDigest == false) and (ARGV[2] == '')) then
			redis.call('xadd', KEYS[1], '*', ARGV[1], ARGV[3])
			redis.call('set', KEYS[2], ARGV[3])
		end
		return true
    `
)

var (
	scriptList = [1]string{pushNewTxDigestToStream}
	nameList   = [1]string{"pushNewTxDigestToStream"}
)

func (r *Redis) InitScript() error {
	r.script = make(map[string]string)
	for i, v := range scriptList {
		shaHash, err := r.ScriptLoad(r.Context(), v).Result()
		if err != nil {
			return err
		}
		r.script[nameList[i]] = shaHash
	}
	return nil
}
