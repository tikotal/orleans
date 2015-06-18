/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;

namespace Orleans.Runtime.MembershipService
{
    internal class MembershipFactory
    {
        private readonly TraceLogger logger;

        internal MembershipFactory()
        {
            logger = TraceLogger.GetLogger("MembershipFactory", TraceLogger.LoggerType.Runtime);
        }

        internal Task CreateMembershipTableProvider(Catalog catalog, Silo silo)
        {
            var livenessType = silo.GlobalConfig.LivenessType;
            logger.Info(ErrorCode.MembershipFactory1, "Creating membership table provider for type={0}", Enum.GetName(typeof(GlobalConfiguration.LivenessProviderType), livenessType));
            if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain))
            {
                return catalog.CreateSystemGrain(
                        Constants.SystemMembershipTableId,
                        typeof(GrainBasedMembershipTable).FullName);
            }
            return TaskDone.Done;
        }

        internal IMembershipOracle CreateMembershipOracle(Silo silo, IMembershipTable membershipTable)
        {
            var livenessType = silo.GlobalConfig.LivenessType;
            logger.Info("Creating membership oracle for type={0}", Enum.GetName(typeof(GlobalConfiguration.LivenessProviderType), livenessType));
            return new MembershipOracle(silo, membershipTable);
        }

        internal IMembershipTable GetMembershipTable(GlobalConfiguration.LivenessProviderType livenessType)
        {
            IMembershipTable membershipTable;
            if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain))
            {
                membershipTable = GrainFactory.Cast<IMembershipTableGrain>(GrainReference.FromGrainId(Constants.SystemMembershipTableId));
            }
            else if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.SqlServer))
            {
                membershipTable = new SqlMembershipTable();
            }
            else if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.AzureTable))
            {
                membershipTable = AssemblyLoader.LoadAndCreateInstance<IMembershipTable>(Constants.ORLEANS_AZURE_UTILS_DLL, logger);
            }
            else if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.ZooKeeper))
            {
                membershipTable = AssemblyLoader.LoadAndCreateInstance<IMembershipTable>(Constants.ORLEANS_ZOOKEEPER_UTILS_DLL, logger);
            }
            else
            {
                throw new NotImplementedException("No membership table provider found for LivenessType=" + livenessType);
            }

            return membershipTable;
        }
    }
}
