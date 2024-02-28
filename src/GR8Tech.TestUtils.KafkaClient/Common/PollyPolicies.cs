using System;
using System.Collections.Generic;
using Polly;

namespace GR8Tech.TestUtils.KafkaClient.Common
{
    internal static class PollyPolicies
    {
        internal static AsyncPolicy AsyncRetryPolicyWithException(int retryCount, TimeSpan sleepDuration)
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(retryCount, i => sleepDuration);
        }
        
        internal static AsyncPolicy<T> AsyncRetryPolicyWithExceptionAndResult<T>(int retryCount, TimeSpan sleepDuration)
        {
            return Policy
                .Handle<Exception>()
                .OrResult<T>(result => EqualityComparer<T>.Default.Equals(result, default!))
                .WaitAndRetryAsync(retryCount, i => sleepDuration);
        }
    }
}

