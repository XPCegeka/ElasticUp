using ElasticUp.Elastic;
using ElasticUp.Operation.Validator;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Validator
{
    [TestFixture]
    public class OperationValidatorIntegrationTest : AbstractIntegrationTest
    {
        private OperationValidator _operationValidator;

        [SetUp]
        public void Setup()
        {
            _operationValidator = OperationValidator.ValidatorFor<OperationValidatorIntegrationTest>(ElasticClient);
        }

        [Test]
        public void GivenExistingIndex_WhenValidatingIfIndexExists_DoesNotThrowException()
        {
            Assert.DoesNotThrow(() => _operationValidator.IndexExists(TestIndex.IndexNameWithVersion()));
        }

        [Test]
        public void GivenUnexistingIndex_WhenValidatingIfIndexExists_DoesThrowException()
        {
            Assert.Throws<ElasticUpException>(() => _operationValidator.IndexExists("fantasy-index"));
        }

        [Test]
        public void GivenAStringValue_ThrowsExceptionWhenNullOrEmpty()
        {
            Assert.Throws<ElasticUpException>(() => _operationValidator.IsNotBlank(null, "message"));
            Assert.Throws<ElasticUpException>(() => _operationValidator.IsNotBlank("", "message"));
            Assert.Throws<ElasticUpException>(() => _operationValidator.IsNotBlank("  ", "message"));
        }

        [Test]
        public void GivenAStringValue_ThrowsExceptionWhenNullOrEmpty_WithGivenMessagePrefixedByClassNameOfOperation()
        {
            try
            {
                _operationValidator.IsNotBlank(null, "message");
                Assert.Fail("should have thrown exception");
            }
            catch (ElasticUpException exception)
            {
                exception.Message.Should().Be("OperationValidatorIntegrationTest: message");
            }
        }
    }
}